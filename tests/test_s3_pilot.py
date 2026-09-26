from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools"))
import s3_pilot as p


class PilotTests(unittest.TestCase):
    def plan(self, **changes):
        return dict({"accountId": "123456789012", "accountPlanType": "FREE",
                     "accountPlanStatus": "ACTIVE", "accountPlanRemainingCredits": {"amount": 10, "unit": "USD"},
                     "accountPlanExpirationDate": (datetime.now(timezone.utc) + timedelta(days=1)).isoformat()}, **changes)

    def test_paid_expired_missing_credit_and_expiring_accounts_are_rejected(self):
        self.assertEqual(p.free_plan(self.plan()), "123456789012")
        cases = [self.plan(accountPlanType="PAID"), self.plan(accountPlanStatus="EXPIRED"),
                 self.plan(accountPlanRemainingCredits={}),
                 self.plan(accountPlanRemainingCredits={"amount": float("nan"), "unit": "USD"}),
                 self.plan(accountPlanExpirationDate=datetime.now(timezone.utc).isoformat()), {}]
        for document in cases:
            with self.subTest(document=document), self.assertRaises((ValueError, TypeError)):
                p.free_plan(document)

    def test_provider_check_stops_before_bucket_access_on_paid_plan(self):
        with patch.object(p, "run", return_value=json.dumps(self.plan(accountPlanType="PAID"))) as run:
            with self.assertRaises(ValueError):
                p.check_aws({})
        self.assertEqual(run.call_count, 1)

    def test_bucket_owner_region_and_versioning_are_checked(self):
        env = {"GLIDER_S3_BUCKET": "glider-pilot-test", "GLIDER_S3_REGION": "eu-central-1"}
        responses = [json.dumps(self.plan()), json.dumps({"LocationConstraint": "eu-central-1"}), ""]
        with patch.object(p, "run", side_effect=responses) as run:
            p.check_aws(env)
        self.assertIn("--expected-bucket-owner", run.call_args.args)
        for last in [{"Status": "Enabled"}, {"Status": "Suspended"}]:
            with patch.object(p, "run", side_effect=responses[:2] + [json.dumps(last)]), self.assertRaises(ValueError):
                p.check_aws(env)
        with patch.object(p, "run", side_effect=responses[:1] + ['{"LocationConstraint":null}']), self.assertRaises(ValueError):
            p.check_aws(env)

    def test_endpoint_and_profile_overrides_cannot_bypass_free_plan_check(self):
        env = p.aws_environment({"AWS_ACCESS_KEY_ID": "id", "AWS_SECRET_ACCESS_KEY": "secret",
                                 "AWS_SESSION_TOKEN": "temporary", "AWS_PROFILE": "paid",
                                 "AWS_ENDPOINT_URL_FREE_TIER": "http://fake", "AWS_ENDPOINT_URL": "http://fake",
                                 "GLIDER_S3_REGION": "eu-central-1", "GLIDER_S3_BUCKET": "glider-pilot-test",
                                 "GLIDER_S3_ENDPOINT": "http://fake"})
        self.assertNotIn("AWS_PROFILE", env)
        self.assertFalse(any(k.startswith("AWS_ENDPOINT_URL") for k in env))
        self.assertEqual(env["AWS_SESSION_TOKEN"], "temporary")
        self.assertEqual(env["GLIDER_S3_ENDPOINT"], "https://s3.eu-central-1.amazonaws.com")
        self.assertEqual(env["AWS_MAX_ATTEMPTS"], "1")

    def test_failure_always_cleans_owned_prefix_without_masking_original(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory); env = {"GLIDER_S3_NAMESPACE": "glider-pilot/test"}
            (output / "owned-prefix.txt").write_text(env["GLIDER_S3_NAMESPACE"])
            with patch.object(p, "run", side_effect=[RuntimeError("original"), RuntimeError("cleanup")]) as run:
                with self.assertRaisesRegex(RuntimeError, "original"):
                    p.exercise(output, env)
            self.assertEqual([c.args[1] for c in run.call_args_list], ["write", "cleanup"])

    def test_nonowned_prefix_is_never_cleaned(self):
        with tempfile.TemporaryDirectory() as directory:
            with patch.object(p, "run", side_effect=RuntimeError("nonempty prefix")) as run:
                with self.assertRaises(RuntimeError):
                    p.exercise(Path(directory), {"GLIDER_S3_NAMESPACE": "glider-pilot/test"})
            self.assertEqual(run.call_count, 1)

    def test_normal_run_uses_separate_write_recovery_and_cleanup_processes(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory); env = {"GLIDER_S3_NAMESPACE": "glider-pilot/test"}
            (output / "owned-prefix.txt").write_text(env["GLIDER_S3_NAMESPACE"])
            with patch.object(p, "run") as run:
                p.exercise(output, env)
            self.assertEqual([c.args[1] for c in run.call_args_list], ["write", "recover", "cleanup"])
            self.assertLessEqual(sum(c.kwargs["timeout"] for c in run.call_args_list), 600)


if __name__ == "__main__":
    unittest.main()
