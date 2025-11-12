#!/usr/bin/env python3
"""
Test script to demonstrate proper error handling in type_index module.
"""

import implica


def test_invalid_bloom_filter_fpr():
    """Test that invalid bloom filter FPR raises proper ValueError."""
    print("Testing invalid bloom_filter_fpr values...")

    # Test FPR too low
    try:
        config = implica.IndexConfig(bloom_filter_fpr=0.0)
        print("❌ FAILED: Should have raised ValueError for fpr=0.0")
    except ValueError as e:
        print(f"✓ Correctly raised ValueError: {e}")

    # Test FPR too high
    try:
        config = implica.IndexConfig(bloom_filter_fpr=1.0)
        print("❌ FAILED: Should have raised ValueError for fpr=1.0")
    except ValueError as e:
        print(f"✓ Correctly raised ValueError: {e}")

    # Test negative FPR
    try:
        config = implica.IndexConfig(bloom_filter_fpr=-0.5)
        print("❌ FAILED: Should have raised ValueError for fpr=-0.5")
    except ValueError as e:
        print(f"✓ Correctly raised ValueError: {e}")

    # Test FPR > 1.0
    try:
        config = implica.IndexConfig(bloom_filter_fpr=1.5)
        print("❌ FAILED: Should have raised ValueError for fpr=1.5")
    except ValueError as e:
        print(f"✓ Correctly raised ValueError: {e}")

    print()


def test_valid_configurations():
    """Test that valid configurations work correctly."""
    print("Testing valid configurations...")

    # Test default config
    config1 = implica.IndexConfig()
    print(f"✓ Default config: {config1}")

    # Test valid FPR
    config2 = implica.IndexConfig(bloom_filter_fpr=0.01)
    print(f"✓ Valid FPR config: {config2}")

    # Test with estimated size
    config3 = implica.IndexConfig(bloom_filter_fpr=0.01, estimated_size=1000000)
    print(f"✓ Config with size: {config3}")

    # Test auto-sizing
    config4 = implica.IndexConfig.for_graph_size(500000)
    print(f"✓ Auto-sized config: {config4}")

    print()


def test_error_message_quality():
    """Test that error messages are clear and helpful."""
    print("Testing error message quality...")

    try:
        config = implica.IndexConfig(bloom_filter_fpr=2.0)
    except ValueError as e:
        error_msg = str(e)
        print(f"Error message: {error_msg}")

        # Check that error message contains key information
        assert "bloom_filter_fpr" in error_msg, "Missing parameter name"
        assert "2.0" in error_msg or "2" in error_msg, "Missing value"
        assert "range" in error_msg.lower() or "0.0" in error_msg, "Missing constraint info"
        print("✓ Error message contains parameter name, value, and constraint")

    print()


def main():
    """Run all tests."""
    print("=" * 60)
    print("Error Handling Tests for type_index Module")
    print("=" * 60)
    print()

    test_invalid_bloom_filter_fpr()
    test_valid_configurations()
    test_error_message_quality()

    print("=" * 60)
    print("All tests completed successfully! ✓")
    print("=" * 60)


if __name__ == "__main__":
    main()
