from scripts.verify_infrastructure import assess_volume_mount


def test_the_development_project_is_left_unchanged() -> None:
    assert assess_volume_mount("titan", "titan_postgres_data ").startswith("ok")
    assert assess_volume_mount("", "titan_postgres_data ").startswith("ok")


def test_a_clone_must_use_its_own_postgres_volume() -> None:
    assert assess_volume_mount("titan-clone", "titan-clone_postgres_data ").startswith("ok")
    shared = assess_volume_mount("titan-clone", "titan_postgres_data ")
    other = assess_volume_mount("titan-clone", "titan-e2e_postgres_data ")
    missing = assess_volume_mount("titan-clone", "something_else ")
    assert shared.startswith("FAIL")
    assert other.startswith("FAIL")
    assert missing.startswith("FAIL")
