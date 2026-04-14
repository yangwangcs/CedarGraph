#!/usr/bin/env python3
"""
CedarGraph Performance Test Results Visualizer
性能测试结果可视化工具

Usage:
    python3 visualize_results.py [results_directory]
"""

import os
import sys
import re
import glob
from datetime import datetime

def parse_result_file(filepath):
    """Parse a single test result file"""
    data = {
        'node_count': 0,
        'total_ops': 0,
        'throughput': 0.0,
        'write_tput': 0.0,
        'read_tput': 0.0,
        'failed_ops': 0
    }
    
    # Extract node count from filename
    match = re.search(r'(\d+)nodes', os.path.basename(filepath))
    if match:
        data['node_count'] = int(match.group(1))
    
    with open(filepath, 'r') as f:
        content = f.read()
        
        # Extract metrics using regex
        total_match = re.search(r'Total:\s+(\d+)', content)
        if total_match:
            data['total_ops'] = int(total_match.group(1))
        
        # Parse throughput - format: "Total:  80682.3 ops/sec"
        tput_match = re.search(r'Total:\s+([\d\.]+)\s+ops/sec', content)
        if tput_match:
            data['throughput'] = float(tput_match.group(1))
        
        # Parse write throughput - format: "Writes: 16009.2 ops/sec"
        write_match = re.search(r'Writes:\s+([\d\.]+)\s+ops/sec', content)
        if write_match:
            data['write_tput'] = float(write_match.group(1))
        
        # Parse read throughput - format: "Reads:  64673.1 ops/sec"
        read_match = re.search(r'Reads:\s+([\d\.]+)\s+ops/sec', content)
        if read_match:
            data['read_tput'] = float(read_match.group(1))
        
        failed_match = re.search(r'Failed:\s+(\d+)', content)
        if failed_match:
            data['failed_ops'] = int(failed_match.group(1))
    
    return data

def calculate_efficiency(results):
    """Calculate scaling efficiency relative to 3-node baseline"""
    baseline = None
    for r in results:
        if r['node_count'] == 3:
            baseline = r['throughput']
            r['efficiency'] = 100.0
            break
    
    if baseline:
        for r in results:
            if r['node_count'] != 3:
                ideal = baseline * r['node_count'] / 3.0
                r['efficiency'] = (r['throughput'] / ideal) * 100 if ideal > 0 else 0.0

def print_ascii_chart(results, title, value_key):
    """Print ASCII bar chart"""
    print(f"\n{title}")
    print("=" * 60)
    
    # Find max value for scaling
    max_val = max(r[value_key] for r in results)
    max_bar_width = 40
    
    # Sort by node count
    sorted_results = sorted(results, key=lambda x: x['node_count'])
    
    for r in sorted_results:
        node_label = f"{r['node_count']} nodes"
        value = r[value_key]
        bar_len = int((value / max_val) * max_bar_width)
        bar = "█" * bar_len
        
        if value_key == 'throughput':
            print(f"{node_label:10} | {bar:<40} {value:>10,.0f} ops/s")
        elif value_key == 'efficiency':
            print(f"{node_label:10} | {bar:<40} {value:>10.1f}%")

def print_summary_table(results):
    """Print summary table"""
    print("\n" + "=" * 80)
    print("PERFORMANCE SUMMARY")
    print("=" * 80)
    print(f"{'Nodes':<8} {'Total Ops':<15} {'Throughput':<20} {'Write Tput':<15} {'Read Tput':<15} {'Efficiency':<12}")
    print("-" * 80)
    
    sorted_results = sorted(results, key=lambda x: x['node_count'])
    
    for r in sorted_results:
        eff_str = f"{r.get('efficiency', 0):.1f}%"
        print(f"{r['node_count']:<8} {r['total_ops']:<15,} {r['throughput']:<20,.1f} {r['write_tput']:<15,.1f} {r['read_tput']:<15,.1f} {eff_str:<12}")
    
    print("=" * 80)

def print_scaling_analysis(results):
    """Print scaling analysis"""
    print("\n" + "=" * 80)
    print("SCALING ANALYSIS")
    print("=" * 80)
    
    sorted_results = sorted(results, key=lambda x: x['node_count'])
    baseline = None
    
    for r in sorted_results:
        nodes = r['node_count']
        tput = r['throughput']
        eff = r.get('efficiency', 0)
        
        if nodes == 3:
            baseline = tput
            print(f"\n3-Node Baseline:")
            print(f"  Throughput: {tput:,.1f} ops/s")
            print(f"  Per-node: {tput/3:,.1f} ops/s")
        else:
            if baseline:
                speedup = tput / baseline
                ideal_speedup = nodes / 3.0
                print(f"\n{nodes}-Node Configuration:")
                print(f"  Throughput: {tput:,.1f} ops/s")
                print(f"  Speedup vs 3-node: {speedup:.2f}x (ideal: {ideal_speedup:.2f}x)")
                print(f"  Scaling efficiency: {eff:.1f}%")
                print(f"  Per-node: {tput/nodes:,.1f} ops/s")
    
    print("\n" + "=" * 80)

def generate_recommendations(results):
    """Generate recommendations based on results"""
    print("\n" + "=" * 80)
    print("RECOMMENDATIONS")
    print("=" * 80)
    
    sorted_results = sorted(results, key=lambda x: x['node_count'])
    
    # Find best efficiency
    best_eff = max(sorted_results, key=lambda x: x.get('efficiency', 0))
    
    # Find best throughput per node
    best_per_node = max(sorted_results, key=lambda x: x['throughput'] / x['node_count'])
    
    print(f"\n1. Best scaling efficiency: {best_eff['node_count']} nodes ({best_eff.get('efficiency', 0):.1f}%)")
    print(f"2. Best per-node throughput: {best_per_node['node_count']} nodes ({best_per_node['throughput']/best_per_node['node_count']:,.1f} ops/s per node)")
    
    # Check if scaling is linear
    efficiencies = [r.get('efficiency', 0) for r in sorted_results if r['node_count'] > 3]
    if efficiencies:
        avg_eff = sum(efficiencies) / len(efficiencies)
        if avg_eff >= 90:
            print(f"3. System shows excellent linear scaling (avg efficiency: {avg_eff:.1f}%)")
        elif avg_eff >= 70:
            print(f"3. System shows good scaling (avg efficiency: {avg_eff:.1f}%)")
            print("   Consider network optimization for better results")
        else:
            print(f"3. Scaling efficiency is below optimal (avg: {avg_eff:.1f}%)")
            print("   Recommended actions:")
            print("   - Check network bandwidth between nodes")
            print("   - Verify client concurrency is sufficient")
            print("   - Review data distribution strategy")
    
    print("\n" + "=" * 80)

def main():
    # Get results directory
    if len(sys.argv) > 1:
        results_dir = sys.argv[1]
    else:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        results_dir = os.path.join(os.path.dirname(script_dir), 'test_results')
    
    if not os.path.exists(results_dir):
        print(f"Error: Results directory not found: {results_dir}")
        print("Usage: python3 visualize_results.py [results_directory]")
        sys.exit(1)
    
    # Find all result files
    pattern = os.path.join(results_dir, 'scaling_test_*nodes_*.txt')
    result_files = glob.glob(pattern)
    
    if not result_files:
        print(f"No result files found in {results_dir}")
        print("Expected files: scaling_test_*nodes_*.txt")
        sys.exit(1)
    
    print("CedarGraph Performance Test Results Visualizer")
    print(f"Found {len(result_files)} result file(s)")
    print(f"Results directory: {results_dir}")
    
    # Parse all results
    results = []
    for filepath in result_files:
        try:
            data = parse_result_file(filepath)
            results.append(data)
        except Exception as e:
            print(f"Warning: Failed to parse {filepath}: {e}")
    
    if not results:
        print("No valid results found")
        sys.exit(1)
    
    # Calculate efficiency
    calculate_efficiency(results)
    
    # Generate visualizations
    print_summary_table(results)
    print_ascii_chart(results, "THROUGHPUT BY NODE COUNT", 'throughput')
    print_ascii_chart(results, "SCALING EFFICIENCY", 'efficiency')
    print_scaling_analysis(results)
    generate_recommendations(results)
    
    print("\nVisualization complete!")
    print(f"Raw data available in: {results_dir}")

if __name__ == '__main__':
    main()
