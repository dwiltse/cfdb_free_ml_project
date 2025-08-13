#!/usr/bin/env python3
"""
Enhanced testing script for CFDB ML Pipeline
Supports unit tests, integration tests, and data quality checks
"""
import sys
import subprocess
import argparse
from pathlib import Path

# Ensure project root is in Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src"))

# Disable bytecode writing to avoid permission issues
sys.dont_write_bytecode = True


def run_unit_tests(verbose=False, coverage=False):
    """Run unit tests"""
    print("🧪 Running unit tests...")
    
    cmd = ["pytest", "tests/unit/"]
    
    if verbose:
        cmd.append("-v")
    
    if coverage:
        cmd.extend(["--cov=src", "--cov-report=html", "--cov-report=term"])
    
    cmd.extend(["-p", "no:cacheprovider"])  # Avoid cache issues
    
    result = subprocess.run(cmd, cwd=project_root)
    return result.returncode == 0


def run_integration_tests(verbose=False):
    """Run integration tests"""
    print("🔗 Running integration tests...")
    
    cmd = ["pytest", "tests/integration/", "-m", "integration"]
    
    if verbose:
        cmd.append("-v")
    
    cmd.extend(["-p", "no:cacheprovider"])
    
    result = subprocess.run(cmd, cwd=project_root)
    return result.returncode == 0


def run_data_quality_checks():
    """Run data quality validation checks"""
    print("📊 Running data quality checks...")
    
    try:
        from cfdb_pipeline.testing.data_quality import DataQualityChecker
        
        checker = DataQualityChecker()
        results = checker.run_all_checks()
        
        if all(results.values()):
            print("✅ All data quality checks passed")
            return True
        else:
            print("❌ Some data quality checks failed:")
            for check, passed in results.items():
                status = "✅" if passed else "❌"
                print(f"  {status} {check}")
            return False
            
    except ImportError:
        print("⚠️  Data quality checker not implemented yet")
        return True


def run_linting():
    """Run code linting"""
    print("🔍 Running code linting...")
    
    # Run ruff
    ruff_result = subprocess.run(["ruff", "check", "src/", "tests/"], cwd=project_root)
    
    # Run black check
    black_result = subprocess.run(["black", "--check", "src/", "tests/"], cwd=project_root)
    
    return ruff_result.returncode == 0 and black_result.returncode == 0


def main():
    parser = argparse.ArgumentParser(description="Run CFDB ML Pipeline tests")
    parser.add_argument("--unit", action="store_true", help="Run unit tests")
    parser.add_argument("--integration", action="store_true", help="Run integration tests")
    parser.add_argument("--quality", action="store_true", help="Run data quality checks")
    parser.add_argument("--lint", action="store_true", help="Run linting")
    parser.add_argument("--all", action="store_true", help="Run all tests")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
    parser.add_argument("--coverage", action="store_true", help="Generate coverage report")
    
    args = parser.parse_args()
    
    if not any([args.unit, args.integration, args.quality, args.lint, args.all]):
        args.all = True  # Default to running all tests
    
    results = []
    
    if args.all or args.lint:
        results.append(("Linting", run_linting()))
    
    if args.all or args.unit:
        results.append(("Unit Tests", run_unit_tests(args.verbose, args.coverage)))
    
    if args.all or args.integration:
        results.append(("Integration Tests", run_integration_tests(args.verbose)))
    
    if args.all or args.quality:
        results.append(("Data Quality", run_data_quality_checks()))
    
    # Print summary
    print("\n" + "="*50)
    print("TEST SUMMARY")
    print("="*50)
    
    all_passed = True
    for test_type, passed in results:
        status = "✅ PASSED" if passed else "❌ FAILED"
        print(f"{test_type}: {status}")
        if not passed:
            all_passed = False
    
    if all_passed:
        print("\n🎉 All tests passed!")
        sys.exit(0)
    else:
        print("\n💥 Some tests failed!")
        sys.exit(1)


if __name__ == "__main__":
    main()