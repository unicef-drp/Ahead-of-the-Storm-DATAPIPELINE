"""
Blob-only "has anything new landed?" precheck used by the precip scheduler (the wind
scheduler has an inline equivalent).

The precip scheduler polls 16x/day but its upstream data (ECMWF `met/<YYYYMMDD>_<HH>/`
zarr archives) only changes about 4x/day. A poll with nothing new still opens several
Snowflake connections (backlog diagnostic, active-countries lookup, the analysis's own
dedup check), each resuming the warehouse. Listing the Blob prefix costs no Snowflake
compute at all, so a confirmed "nothing new" poll can exit before touching Snowflake.

A skip is only taken when ALL of the following hold:
  1. No blob under the watched prefix has a last_modified after the saved watermark.
  2. The last real check found no outstanding backlog (backlog_confirmed_clear).
  3. No more than `max_skip_hours` have passed since the last real check, so a candidate
     that became eligible for a reason unrelated to new Blob data (a retry lock window
     expiring, a deferred country) is still found within a bounded time.

The watermark saved after a real check that processed nothing is held back by
`settle_minutes`. A forecast archive is uploaded before the Snowflake pointer row that
makes it discoverable is written, so a check that runs in that gap would otherwise
advance the watermark past data it could not yet see and skip it on later polls. Holding
the watermark back keeps such a blob "new" until it is old enough to have settled.

Every failure mode (missing state, unreadable state, Blob listing error, unparseable
timestamp) falls through to a normal real check. Skipping a real forecast cycle to save
warehouse time would be a far worse outcome than the cost this exists to reduce.
"""

import json
import os
import re
from datetime import datetime, timedelta, timezone


class BlobPrecheck:
    def __init__(self, name, prefix, name_regex, state_path, data_store, logger,
                 max_skip_hours=4, settle_minutes=20):
        self.name = name
        self.prefix = prefix
        self._name_re = re.compile(name_regex)
        self.state_path = state_path
        self._store = data_store
        self.logger = logger
        self.max_skip_hours = max_skip_hours
        self.settle_minutes = settle_minutes

    def _list_last_modified(self):
        """Real last_modified of every watched blob, one list_blobs() call."""
        from azure.storage.blob import BlobServiceClient
        client = BlobServiceClient(
            account_url=os.environ["HAZARD_BLOB_ACCOUNT_URL"],
            credential=os.environ["HAZARD_BLOB_SAS_TOKEN"],
        )
        container = client.get_container_client(os.environ["HAZARD_BLOB_CONTAINER"])
        return [b.last_modified for b in container.list_blobs(name_starts_with=self.prefix)
                if b.last_modified is not None and self._name_re.match(b.name)]

    def _load_state(self):
        try:
            if not self._store.file_exists(self.state_path):
                return None
            raw = self._store.read_file(self.state_path)
            return json.loads(raw.decode("utf-8") if isinstance(raw, bytes) else raw)
        except Exception as e:
            self.logger.warning(f"Could not read {self.name} Blob precheck state, treating as unknown: {e}")
            return None

    def should_skip(self):
        """Returns (should_skip, newest_seen_iso_or_None, reason)."""
        state = self._load_state()
        if state is None:
            return False, None, "no prior precheck state (first run, or a prior save failed)"
        if not state.get("backlog_confirmed_clear"):
            return False, None, "last real check still had outstanding backlog"
        try:
            last_real = datetime.fromisoformat(state["last_real_check_at"])
            prior_watermark = datetime.fromisoformat(state["watermark"])
        except Exception:
            return False, None, "prior state missing a valid watermark or last_real_check_at"
        if datetime.now(timezone.utc) - last_real > timedelta(hours=self.max_skip_hours):
            return False, None, f"more than {self.max_skip_hours}h since the last real check, forcing a recheck"
        try:
            stamps = self._list_last_modified()
        except Exception as e:
            return False, None, f"Blob listing failed, cannot confirm no new data: {e}"
        if not stamps:
            return False, None, "no watched blobs found, running a real check"
        newest = max(stamps)
        if newest > prior_watermark:
            return False, newest.isoformat(), "new data found since the last real check"
        return True, newest.isoformat(), "no new data since the last real check"

    def current_newest_iso(self):
        """Fresh newest last_modified for seeding the watermark when should_skip() could not."""
        try:
            stamps = self._list_last_modified()
            return max(stamps).isoformat() if stamps else None
        except Exception as e:
            self.logger.warning(f"Could not read {self.name} Blob watermark: {e}")
            return None

    def save_after_real_check(self, newest_seen_iso, backlog_confirmed_clear, processed_something):
        """Saves state after a genuine check. Best effort: a failed write only means the next
        poll cannot skip and checks for real."""
        try:
            now = datetime.now(timezone.utc)
            watermark = newest_seen_iso
            if watermark is not None and not processed_something:
                held_back = (now - timedelta(minutes=self.settle_minutes)).isoformat()
                watermark = min(watermark, held_back)
            self._write(watermark, backlog_confirmed_clear, now.isoformat())
        except Exception as e:
            self.logger.warning(f"Could not save {self.name} Blob precheck state: {e}")

    def save_after_skip(self, newest_seen_iso):
        """A skip must leave last_real_check_at exactly as the last real check left it, or a
        chain of skips could push the max_skip_hours bound out indefinitely."""
        try:
            state = self._load_state() or {}
            self._write(newest_seen_iso or state.get("watermark"), True, state.get("last_real_check_at"))
        except Exception as e:
            self.logger.warning(f"Could not save {self.name} Blob precheck state: {e}")

    def _write(self, watermark_iso, backlog_confirmed_clear, last_real_check_at_iso):
        self._store.write_file(self.state_path, json.dumps({
            "watermark": watermark_iso,
            "backlog_confirmed_clear": backlog_confirmed_clear,
            "last_real_check_at": last_real_check_at_iso,
        }))
