from scan_secrets import main


def test_tracked_files_do_not_contain_secret_markers() -> None:
    main()
