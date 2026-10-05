import math
import random
import uuid
from datetime import datetime, timezone

from locust import HttpUser, LoadTestShape, between, task

HOST = "http://localhost:8081"

INITIAL_USERS = 100
USERS_PER_STEP = 50
STEP_INTERVAL_SECONDS = 15
MAX_USERS = 1000
HOLD_SECONDS = 30

MIN_WAIT_SECONDS = 0.5
MAX_WAIT_SECONDS = 2.0

PAGES = ["/", "/pricing", "/docs", "/blog", "/contact", "/login"]
REFERRERS = ["google", "newsletter", "twitter", "direct"]

STEPS_TO_REACH_MAX_USERS = math.ceil((MAX_USERS - INITIAL_USERS) / USERS_PER_STEP)
RAMP_SECONDS = STEPS_TO_REACH_MAX_USERS * STEP_INTERVAL_SECONDS


class PageViewUser(HttpUser):
    host = HOST
    wait_time = between(MIN_WAIT_SECONDS, MAX_WAIT_SECONDS)

    def on_start(self):
        self.user_id = f"user-{uuid.uuid4().hex[:12]}"

    @task
    def view_a_random_page(self):
        activity = {
            "user_id": self.user_id,
            "activity_type": "page_view",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "metadata": {
                "page_url": random.choice(PAGES),
                "referrer": random.choice(REFERRERS),
            },
        }

        with self.client.post(
            "/user_activities",
            json=activity,
            name="POST /user_activities",
            catch_response=True,
        ) as response:
            if response.status_code != 202:
                response.failure(f"expected 202, got {response.status_code}: {response.text}")


class GradualRamp(LoadTestShape):
    def tick(self):
        elapsed_seconds = self.get_run_time()

        if elapsed_seconds >= RAMP_SECONDS + HOLD_SECONDS:
            return None

        steps_taken = int(elapsed_seconds // STEP_INTERVAL_SECONDS)
        target_users = min(INITIAL_USERS + steps_taken * USERS_PER_STEP, MAX_USERS)

        return target_users, USERS_PER_STEP
