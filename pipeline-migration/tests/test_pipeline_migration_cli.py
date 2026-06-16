import subprocess
import sys
import unittest
from pathlib import Path


SKILL_DIR = Path(__file__).resolve().parents[1]
CLI = SKILL_DIR / "bin" / "pipeline-migration"


def run_cli(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(CLI), *args],
        check=False,
        capture_output=True,
        text=True,
    )


class PipelineMigrationCliTests(unittest.TestCase):
    def test_check_package_passes(self) -> None:
        result = run_cli("check-package")

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("Package check passed", result.stdout)

    def test_assess_prompt_includes_workspace(self) -> None:
        result = run_cli("assess", "--synapse-workspace", "contoso-synapse")

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("contoso-synapse", result.stdout)
        self.assertIn("read-only", result.stdout)

    def test_migrate_prompt_keeps_safe_defaults(self) -> None:
        result = run_cli(
            "migrate",
            "--synapse-workspace",
            "contoso-synapse",
            "--fabric-workspace",
            "Contoso Fabric",
        )

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("_migrated", result.stdout)
        self.assertIn("Do not overwrite existing Fabric items", result.stdout)
        self.assertIn("Do not migrate triggers or schedules", result.stdout)

    def test_validate_prompt_includes_workspace(self) -> None:
        result = run_cli("validate", "--fabric-workspace", "Contoso Fabric")

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("Contoso Fabric", result.stdout)
        self.assertIn("validation-testing.md", result.stdout)


if __name__ == "__main__":
    unittest.main()
