from __future__ import annotations

from dmq.retry import ExponentialBackoff, FixedDelayBackoff, LinearBackoff, RetryPolicy


class TestExponentialBackoff:
	def test_basic_delay(self):
		backoff = ExponentialBackoff(base=2.0, jitter=False)
		assert backoff.calculate_delay(0) == 1.0  # 2^0
		assert backoff.calculate_delay(1) == 2.0  # 2^1
		assert backoff.calculate_delay(3) == 8.0  # 2^3

	def test_max_delay(self):
		backoff = ExponentialBackoff(base=2.0, max_delay=10.0, jitter=False)
		assert backoff.calculate_delay(100) == 10.0

	def test_jitter_changes_delay(self):
		backoff = ExponentialBackoff(base=2.0, jitter=True)
		delays = {backoff.calculate_delay(5) for _ in range(20)}
		# With jitter, we should get varying values
		assert len(delays) > 1


class TestFixedDelayBackoff:
	def test_constant_delay(self):
		backoff = FixedDelayBackoff(delay=5.0)
		assert backoff.calculate_delay(0) == 5.0
		assert backoff.calculate_delay(10) == 5.0


class TestLinearBackoff:
	def test_linear_increase(self):
		backoff = LinearBackoff(base=1.0, increment=2.0)
		assert backoff.calculate_delay(0) == 1.0
		assert backoff.calculate_delay(1) == 3.0
		assert backoff.calculate_delay(2) == 5.0

	def test_max_delay(self):
		backoff = LinearBackoff(base=1.0, increment=2.0, max_delay=4.0)
		assert backoff.calculate_delay(10) == 4.0


class TestRetryPolicy:
	def test_should_retry_within_limit(self):
		policy = RetryPolicy(max_retries=3)
		assert policy.should_retry(ValueError("err"), retry_count=0) is True
		assert policy.should_retry(ValueError("err"), retry_count=2) is True

	def test_should_not_retry_at_limit(self):
		policy = RetryPolicy(max_retries=3)
		assert policy.should_retry(ValueError("err"), retry_count=3) is False

	def test_dont_retry_on_specific_exception(self):
		policy = RetryPolicy(max_retries=3, dont_retry_on=(TypeError,))
		assert policy.should_retry(TypeError("err"), retry_count=0) is False
		assert policy.should_retry(ValueError("err"), retry_count=0) is True

	def test_retry_only_on_specific_exception(self):
		policy = RetryPolicy(max_retries=3, retry_on=(ValueError,))
		assert policy.should_retry(ValueError("err"), retry_count=0) is True
		assert policy.should_retry(TypeError("err"), retry_count=0) is False

	def test_get_delay(self):
		policy = RetryPolicy(backoff=FixedDelayBackoff(delay=1.5))
		assert policy.get_delay(0) == 1.5
