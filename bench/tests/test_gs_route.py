import os

from gsbench.routes.gs import BUILD_INFO, find_worker_jar


def test_worker_jar_found_beside_cli_jar_or_in_reactor(tmp_path):
    cli = tmp_path / "gefrierschrank-cli" / "target" / "gefrierschrank-cli-1.0.0.jar"
    cli.parent.mkdir(parents=True)
    cli.touch()
    assert find_worker_jar(cli) is None
    worker_dir = tmp_path / "gefrierschrank-lucene9" / "target"
    worker_dir.mkdir(parents=True)
    (worker_dir / "original-gefrierschrank-lucene9-1.0.0.jar").touch()
    assert find_worker_jar(cli) is None
    (worker_dir / "gefrierschrank-lucene9-1.0.0.jar").touch()
    assert find_worker_jar(cli) == worker_dir / "gefrierschrank-lucene9-1.0.0.jar"
    (worker_dir / "gefrierschrank-lucene9-1.10.0.jar").touch()
    assert find_worker_jar(cli) == worker_dir / "gefrierschrank-lucene9-1.0.0.jar"  # same version, not "1.9" > "1.10"
    (cli.parent / "gefrierschrank-lucene9-1.0.0.jar").touch()
    assert find_worker_jar(cli) == cli.parent / "gefrierschrank-lucene9-1.0.0.jar"
    other = tmp_path / "x" / "gefrierschrank-cli-2.0.jar"
    other.parent.mkdir()
    other.touch()
    for k, name in enumerate(["gefrierschrank-lucene9-1.9.jar", "gefrierschrank-lucene9-1.10.jar"]):
        (other.parent / name).touch()
        os.utime(other.parent / name, (1000 + k, 1000 + k))
    assert find_worker_jar(other) == other.parent / "gefrierschrank-lucene9-1.10.jar"


def test_build_info_line():
    m = BUILD_INFO.match("gefrierschrank 1.0.0 (built 2026-09-27T12:04:44Z)")
    assert m.group(1) == "1.0.0" and m.group(2) == "2026-09-27T12:04:44Z"
