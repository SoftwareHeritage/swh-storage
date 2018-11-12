from hypothesis import settings

# define tests profile. Full documentation is at:
# https://hypothesis.readthedocs.io/en/latest/settings.html#settings-profiles
settings.register_profile("ci", max_examples=5, deadline=5000)
settings.register_profile("dev", max_examples=20, deadline=5000)
