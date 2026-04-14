#!/usr/bin/env python3
"""
CedarGraph Temporal Graph Performance Results Visualizer
时态图性能测试结果可视化工具

Usage:
    python3 visualize_temporal_results.py [results_directory]
"""

import os
import sys
import re
import glob
from collections import defaultdict

def parse_temporal_result_file(filepath):
    """Parse temporal graph benchmark result file"""
    data = {
        'node_count': 0,
        'test_type': '',
        'total_queries': 0,
        'throughput': 0.0,
        'point_throughput': 0.0,
        'range_throughput': 0.0,
        'analytics_throughput': 0.0,
        'write_throughput': 0.0,
    }
    
    # Extract info from filename
    filename = os.path.basename(filepath)
    match = re.search(r'(\d+)nodes_(\w+)_', filename)
    if match:
        data['node_count'] = int(match.group(1))
        data['test_type'] = match.group(2)
    
    with open(filepath, 'r') as f:
        content = f.read()
        
        # Extract throughput values
        total_match = re.search(r'Total Throughput\s*\|\s*([\d.]+)', content)
        if total_match:
            data['throughput'] = float(total_match.group(1))
        
        point_match = re.search(r'Point Query Throughput\s*\|\s*([\d.]+)', content)
        if point_match:
            data['point_throughput'] = float(point_match.group(1))
        
        range_match = re.search(r'Range Query Throughput\s*\|\s*([\d.]+)', content)
        if range_match:
            data['range_throughput'] = float(range_match.group(1))
        
        analytics_match = re.search(r'Analytics Throughput\s*\|\s*([\d.]+)', content)
        if analytics_match:
            data['analytics_throughput'] = float(analytics_match.group(1))
        
        write_match = re.search(r'Write Throughput\s*\|\s*([\d.]+)', content)
        if write_match:
            data['write_throughput'] = float(write_match.group(1))
        
        total_queries_match = re.search(r'Total Queries\s*\|\s*(\d+)', content)
        if total_queries_match:
            data['total_queries'] = int(total_queries_match.group(1))
    
    return data

def print_comparison_table(results_by_type):
    """Print comparison table for each test type"""
    
    for test_type, results in sorted(results_by_type.items()):
        print(f"\n{'='*80}")
        print(f"Test Type: {test_type.upper()}")
        print('='*80)
        
        # Sort by node count
        sorted_results = sorted(results, key=lambda x: x['node_count'])
        
        # Print header
        print(f"{'Nodes':<8} {'Total QPS':<15} {'Point QPS':<15} {'Range QPS':<15} {'Analytics QPS':<15} {'Write QPS':<15}")
        print('-'*80)
        
        # Print data
        for r in sorted_results:
            print(f"{r['node_count']:<8} {r['throughput']:<15.1f} {r['point_throughput']:<15.1f} "
                  f"{r['range_throughput']:<15.1f} {r['analytics_throughput']:<15.1f} {r['write_throughput']:<15.1f}")
        
        # Calculate scaling efficiency
        if len(sorted_results) >= 2:
            baseline = next((r for r in sorted_results if r['node_count'] == 3), None)
            if baseline:
                print('\nScaling Efficiency (vs 3-node baseline):')
                for r in sorted_results:
                    if r['node_count'] != 3:
                        ideal = baseline['throughput'] * r['node_count'] / 3.0
                        efficiency = (r['throughput'] / ideal) * 100 if ideal > 0 else 0
                        speedup = r['throughput'] / baseline['throughput']
                        print(f"  {r['node_count']} nodes: {efficiency:.1f}% efficiency, {speedup:.2f}x speedup")

def print_ascii_chart(title, data_by_nodes, value_key):
    """Print ASCII bar chart"""
    print(f"\n{title}")
    print('='*60)
    
    max_val = max(data_by_nodes.values()) if data_by_nodes else 0
    if max_val == 0:
        print("No data available")
        return
    
    max_bar_width = 40
    
    for nodes in sorted(data_by_nodes.keys()):
        value = data_by_nodes[nodes]
        bar_len = int((value / max_val) * max_bar_width)
        bar = "█" * bar_len
        print(f"{nodes} nodes  | {bar:<40} {value:>10,.0f}")

def generate_recommendations(results_by_type):
    """Generate recommendations based on results"""
    print(f"\n{'='*80}")
    print("RECOMMENDATIONS")
    print('='*80)
    
    # Find best configuration for each test type
    print("\n1. Optimal Configuration by Workload Type:")
    for test_type, results in sorted(results_by_type.items()):
        if not results:
            continue
        best = max(results, key=lambda x: x['throughput'])
        print(f"   {test_type:15s}: {best['node_count']} nodes ({best['throughput']:.0f} qps)")
    
    # Analyze scaling trends
    print("\n2. Scaling Analysis:")
    
    # Check if there's a 'mixed' workload to analyze overall performance
    if 'mixed' in results_by_type:
        mixed_results = results_by_type['mixed']
        if len(mixed_results) >= 2:
            sorted_mixed = sorted(mixed_results, key=lambda x: x['node_count'])
            baseline = next((r for r in sorted_mixed if r['node_count'] == 3), None)
            
            if baseline:
                avg_efficiency = 0
                count = 0
                for r in sorted_mixed:
                    if r['node_count'] != 3:
                        ideal = baseline['throughput'] * r['node_count'] / 3.0
                        efficiency = (r['throughput'] / ideal) * 100 if ideal > 0 else 0
                        avg_efficiency += efficiency
                        count += 1
                
                if count > 0:
                    avg_efficiency /= count
                    if avg_efficiency >= 90:
                        print(f"   Excellent linear scaling detected (avg efficiency: {avg_efficiency:.1f}%)")
                        print("   The system scales near-perfectly with additional nodes")
                    elif avg_efficiency >= 70:
                        print(f"   Good scaling efficiency (avg: {avg_efficiency:.1f}%)")
                        print("   Consider network optimization for better results")
                    else:
                        print(f"   Sub-optimal scaling (avg: {avg_efficiency:.1f}%)")
                        print("   Review data partitioning and query distribution")
    
    print("\n3. Workload-Specific Recommendations:")
    
    # Check analytics performance
    if 'analytics' in results_by_type:
        analytics_results = results_by_type['analytics']
        if analytics_results:
            avg_analytics = sum(r['throughput'] for r in analytics_results) / len(analytics_results)
            if avg_analytics < 100:
                print("   Analytics queries show low throughput - consider:")
                print("   - Increasing analytics query parallelism")
                print("   - Using materialized views for common patterns")
            else:
                print("   Analytics performance is good")
    
    # Check write performance
    if 'write' in results_by_type:
        write_results = results_by_type['write']
        if write_results:
            avg_write = sum(r['throughput'] for r in write_results) / len(write_results)
            print(f"   Average write throughput: {avg_write:.0f} ops/s")
            if avg_write < 1000:
                print("   Consider batching writes for better throughput")
    
    print('='*80)

def main():
    # Get results directory
    if len(sys.argv) > 1:
        results_dir = sys.argv[1]
    else:
        script_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        results_dir = os.path.join(script_dir, 'test_results', 'temporal')
    
    if not os.path.exists(results_dir):
        print(f"Error: Results directory not found: {results_dir}")
        print("Usage: python3 visualize_temporal_results.py [results_directory]")
        sys.exit(1)
    
    # Find all result files
    pattern = os.path.join(results_dir, 'temporal_*nodes_*.md')
    result_files = glob.glob(pattern)
    
    if not result_files:
        print(f"No temporal result files found in {results_dir}")
        print("Expected files: temporal_*nodes_*.md")
        sys.exit(1)
    
    print("CedarGraph Temporal Graph Performance Visualizer")
    print(f"Found {len(result_files)} result file(s)")
    print(f"Results directory: {results_dir}")
    
    # Parse all results and group by test type
    results_by_type = defaultdict(list)
    
    for filepath in result_files:
        try:
            data = parse_temporal_result_file(filepath)
            if data['test_type']:
                results_by_type[data['test_type']].append(data)
        except Exception as e:
            print(f"Warning: Failed to parse {filepath}: {e}")
    
    if not results_by_type:
        print("No valid results found")
        sys.exit(1)
    
    # Print comparison tables
    print_comparison_table(results_by_type)
    
    # Print ASCII charts for mixed workload
    if 'mixed' in results_by_type:
        mixed_results = results_by_type['mixed']
        
        throughput_by_nodes = {r['node_count']: r['throughput'] for r in mixed_results}
        point_by_nodes = {r['node_count']: r['point_throughput'] for r in mixed_results}
        range_by_nodes = {r['node_count']: r['range_throughput'] for r in mixed_results}
        analytics_by_nodes = {r['node_count']: r['analytics_throughput'] for r in mixed_results}
        
        print_ascii_chart("\nMIXED WORKLOAD - TOTAL THROUGHPUT", throughput_by_nodes, 'throughput')
        print_ascii_chart("\nPOINT QUERY THROUGHPUT", point_by_nodes, 'point_throughput')
        print_ascii_chart("\nRANGE QUERY THROUGHPUT", range_by_nodes, 'range_throughput')
        print_ascii_chart("\nANALYTICS THROUGHPUT", analytics_by_nodes, 'analytics_throughput')
    
    # Generate recommendations
    generate_recommendations(results_by_type)
    
    print("\nVisualization complete!")

if __name__ == '__main__':
    main()
