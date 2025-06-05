import os
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
from pathlib import Path
import argparse
import re

def extract_topic_from_filename(filename):
    """Extract topic name from latency log filename."""
    # Expected format: subscriber-{topic}-{instance}.latency.log or .csv
    match = re.search(r'subscriber-(.*?)-\d+\.latency\.(log|csv)', filename)
    if match:
        return match.group(1)
    return "unknown"

def load_latency_data(folder_path):
    """Load all latency data from CSV files in the given folder."""
    folder_path = Path(folder_path)
    data_by_topic = {}
    
    # Find all latency log files
    latency_files = list(folder_path.glob('*.latency.csv')) + list(folder_path.glob('*.latency.log'))
    
    if not latency_files:
        print(f"No latency files found in {folder_path}")
        return data_by_topic
    
    for file_path in latency_files:
        try:
            # Extract topic name from filename
            topic = extract_topic_from_filename(file_path.name)
            
            # Load data - expected format: timestamp,topic,message_id,send_time,receive_time,latency_ms
            df = pd.read_csv(file_path, header=None)
            
            # If the file has a header row, parse accordingly
            if isinstance(df.iloc[0, 0], str) and ',' in df.iloc[0, 0]:
                df = pd.read_csv(file_path)
            else:
                # Otherwise assign column names
                df.columns = ['timestamp', 'topic', 'message_id', 'send_time', 'receive_time', 'latency_ms']
            
            # Extract actual topic from the data if available
            if 'topic' in df.columns:
                # Group by the actual message topic
                for msg_topic, group in df.groupby('topic'):
                    topic_key = f"{topic}-{msg_topic}"
                    if topic_key not in data_by_topic:
                        data_by_topic[topic_key] = []
                    data_by_topic[topic_key].append(group)
            else:
                # Use the filename-derived topic
                if topic not in data_by_topic:
                    data_by_topic[topic] = []
                data_by_topic[topic].append(df)
                
            print(f"Loaded {len(df)} records from {file_path}")
            
        except Exception as e:
            print(f"Error loading {file_path}: {str(e)}")
    
    # Combine all dataframes for each topic
    for topic in data_by_topic:
        data_by_topic[topic] = pd.concat(data_by_topic[topic], ignore_index=True)
        # Ensure latency is numeric
        data_by_topic[topic]['latency_ms'] = pd.to_numeric(data_by_topic[topic]['latency_ms'], errors='coerce')
    
    return data_by_topic

def create_comparison_charts(folder1_data, folder2_data, output_folder, folder1_name="Run 1", folder2_name="Run 2"):
    """Create comparison charts between two sets of latency data."""
    output_folder = Path(output_folder)
    output_folder.mkdir(exist_ok=True, parents=True)
    
    # Combined topics from both folders
    all_topics = set(folder1_data.keys()) | set(folder2_data.keys())
    
    # Overall statistics table
    stats_data = []
    
    # 1. Create individual topic comparison charts
    for topic in all_topics:
        fig, ax = plt.figure(figsize=(12, 6)), plt.gca()
        
        # Plot data from folder 1 if available
        if topic in folder1_data:
            latency1 = folder1_data[topic]['latency_ms']
            sns.kdeplot(latency1, ax=ax, label=f"{folder1_name} (mean: {latency1.mean():.2f}ms)", shade=True)
            stats_data.append({
                'Topic': topic,
                'Run': folder1_name,
                'Count': len(latency1),
                'Mean (ms)': latency1.mean(),
                'Median (ms)': latency1.median(),
                'Min (ms)': latency1.min(),
                'Max (ms)': latency1.max(),
                'Std Dev (ms)': latency1.std()
            })
        
        # Plot data from folder 2 if available
        if topic in folder2_data:
            latency2 = folder2_data[topic]['latency_ms']
            sns.kdeplot(latency2, ax=ax, label=f"{folder2_name} (mean: {latency2.mean():.2f}ms)", shade=True)
            stats_data.append({
                'Topic': topic,
                'Run': folder2_name,
                'Count': len(latency2),
                'Mean (ms)': latency2.mean(),
                'Median (ms)': latency2.median(),
                'Min (ms)': latency2.min(),
                'Max (ms)': latency2.max(),
                'Std Dev (ms)': latency2.std()
            })
        
        plt.title(f"Latency Distribution for Topic: {topic}")
        plt.xlabel("Latency (ms)")
        plt.ylabel("Density")
        plt.legend()
        plt.tight_layout()
        
        # Save the chart
        output_file = output_folder / f"latency_comparison_{topic.replace('/', '_')}.png"
        plt.savefig(output_file)
        plt.close()
        print(f"Created comparison chart for topic {topic}: {output_file}")
    
    # 2. Create summary chart comparing mean latencies across topics
    topic_list = list(all_topics)
    means1 = [folder1_data[t]['latency_ms'].mean() if t in folder1_data else 0 for t in topic_list]
    means2 = [folder2_data[t]['latency_ms'].mean() if t in folder2_data else 0 for t in topic_list]
    
    # Bar chart for mean latency comparison
    fig, ax = plt.subplots(figsize=(14, 8))
    x = np.arange(len(topic_list))
    width = 0.35
    
    ax.bar(x - width/2, means1, width, label=folder1_name)
    ax.bar(x + width/2, means2, width, label=folder2_name)
    
    ax.set_title('Mean Latency Comparison Across Topics')
    ax.set_xlabel('Topic')
    ax.set_ylabel('Mean Latency (ms)')
    ax.set_xticks(x)
    ax.set_xticklabels(topic_list, rotation=45, ha='right')
    ax.legend()
    
    plt.tight_layout()
    output_file = output_folder / "latency_means_comparison.png"
    plt.savefig(output_file)
    plt.close()
    print(f"Created mean latency comparison chart: {output_file}")
    
    # 3. Create box plots for comparison of distributions
    fig, ax = plt.subplots(figsize=(14, 8))
    
    # Prepare data for boxplot
    boxplot_data = []
    boxplot_labels = []
    
    for topic in topic_list:
        if topic in folder1_data:
            boxplot_data.append(folder1_data[topic]['latency_ms'])
            boxplot_labels.append(f"{topic}\n({folder1_name})")
        
        if topic in folder2_data:
            boxplot_data.append(folder2_data[topic]['latency_ms'])
            boxplot_labels.append(f"{topic}\n({folder2_name})")
    
    ax.boxplot(boxplot_data, labels=boxplot_labels, showfliers=False)
    ax.set_title('Latency Distribution Comparison (Box Plot)')
    ax.set_ylabel('Latency (ms)')
    ax.set_xticklabels(boxplot_labels, rotation=45, ha='right')
    
    plt.tight_layout()
    output_file = output_folder / "latency_boxplot_comparison.png"
    plt.savefig(output_file)
    plt.close()
    print(f"Created boxplot comparison chart: {output_file}")
    
    # 4. Save statistics to CSV
    stats_df = pd.DataFrame(stats_data)
    stats_output = output_folder / "latency_statistics.csv"
    stats_df.to_csv(stats_output, index=False)
    print(f"Saved statistics to: {stats_output}")
    
    # 5. Create heatmap of latency variation over time (if timestamps are available)
    for topic in all_topics:
        try:
            fig, ax = plt.subplots(figsize=(14, 6))
            
            if topic in folder1_data and 'send_time' in folder1_data[topic].columns:
                df1 = folder1_data[topic].copy()
                df1['datetime'] = pd.to_datetime(df1['send_time'], unit='ms')
                df1 = df1.set_index('datetime')
                df1 = df1.sort_index()
                
                # Resample to smooth the data
                resampled1 = df1['latency_ms'].resample('1S').mean()
                resampled1.plot(ax=ax, label=f"{folder1_name}")
            
            if topic in folder2_data and 'send_time' in folder2_data[topic].columns:
                df2 = folder2_data[topic].copy()
                df2['datetime'] = pd.to_datetime(df2['send_time'], unit='ms')
                df2 = df2.set_index('datetime')
                df2 = df2.sort_index()
                
                # Resample to smooth the data
                resampled2 = df2['latency_ms'].resample('1S').mean()
                resampled2.plot(ax=ax, label=f"{folder2_name}")
            
            ax.set_title(f"Latency Over Time for Topic: {topic}")
            ax.set_xlabel("Time")
            ax.set_ylabel("Latency (ms)")
            ax.legend()
            
            plt.tight_layout()
            output_file = output_folder / f"latency_time_{topic.replace('/', '_')}.png"
            plt.savefig(output_file)
            plt.close()
            print(f"Created time-series chart for topic {topic}: {output_file}")
            
        except Exception as e:
            print(f"Could not create time-series chart for {topic}: {str(e)}")

def main():
    parser = argparse.ArgumentParser(description='Compare MQTT latency logs between two folders')
    parser.add_argument('folder1', help='Path to first folder with latency logs')
    parser.add_argument('folder2', help='Path to second folder with latency logs')
    parser.add_argument('--output', '-o', default='./latency_analysis', help='Output folder for charts')
    parser.add_argument('--name1', default='Run 1', help='Name for the first run')
    parser.add_argument('--name2', default='Run 2', help='Name for the second run')
    
    args = parser.parse_args()
    
    print(f"Loading data from {args.folder1}...")
    folder1_data = load_latency_data(args.folder1)
    
    print(f"Loading data from {args.folder2}...")
    folder2_data = load_latency_data(args.folder2)
    
    print("Creating comparison charts...")
    create_comparison_charts(folder1_data, folder2_data, args.output, args.name1, args.name2)
    
    print(f"Analysis complete! Charts saved to: {args.output}")

if __name__ == "__main__":
    main()