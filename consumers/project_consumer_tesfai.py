# project_consumer_tesfai.py

import json 
import os
import matplotlib.pyplot as plt
import matplotlib.patheffects as path_effects
import mplcursors
from collections import defaultdict
from time import sleep

# Data structure to store sentiment by category
category_sentiment = defaultdict(lambda: {'total_sentiment': 0, 'message_count': 0})
processed_messages = set()  # Store processed message timestamps to avoid re-processing

# Set up live visuals
fig, ax = plt.subplots(figsize=(6.4, 4.8))  # Original figsize applied
plt.ion()  # Interactive mode

def update_chart():
    """Update the live chart with the latest average sentiment per category."""
    # Clear the previous chart
    ax.clear()

    # Get categories and their average sentiment
    categories = list(category_sentiment.keys())
    average_sentiments = [data['total_sentiment'] / data['message_count'] for data in category_sentiment.values()]

    # Create the bar chart with shadow and gradient color based on sentiment
    bars = ax.bar(categories, average_sentiments, color=plt.cm.viridis(average_sentiments))  # Gradient color
    
    # Adding shadow to bars (increased prominence)
    for bar in bars:
        bar.set_path_effects([path_effects.withStroke(linewidth=3, foreground='gray')])

    # Add data labels to the bars
    for bar in bars:
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width() / 2, height + 0.01, f'{height:.2f}', ha='center', va='bottom', fontsize=10)

    # Set labels and title
    ax.set_xlabel("Categories")
    ax.set_ylabel("Average Sentiment")
    ax.set_title("Real-Time Average Sentiment by Category")

    # Rotate x-tick labels
    ax.set_xticklabels(categories, rotation=45, ha="right")

    # Adjust layout
    plt.tight_layout()

    # Add interactive hover tooltips
    mplcursors.cursor(bars, hover=True).connect(
        "add", lambda sel: sel.annotation.set_text(f'{categories[sel.index]}\nSentiment: {average_sentiments[sel.index]:.2f}')
    )

    # Draw the chart
    plt.draw()

    # Pause for real-time update
    plt.pause(0.01)

def process_message(message: str) -> None:
    """Process each incoming message to update sentiment data and chart."""
    try:
        # Parse the message
        message_dict = json.loads(message)

        if isinstance(message_dict, dict):
            timestamp = message_dict.get("timestamp")

            # Skip messages that have already been processed (based on timestamp)
            if timestamp in processed_messages:
                return
            processed_messages.add(timestamp)

            category = message_dict.get("category", "unknown")
            sentiment = message_dict.get("sentiment", 0)

            # Update category sentiment
            category_sentiment[category]['total_sentiment'] += sentiment
            category_sentiment[category]['message_count'] += 1

            # Update the chart with new data
            update_chart()

        else:
            print(f"Expected a dictionary but got: {type(message_dict)}")

    except json.JSONDecodeError:
        print(f"Invalid JSON message: {message}")
    except Exception as e:
        print(f"Error processing message: {e}")

def main():
    data_file = "C:/Users/su_te/buzzline-04-tesfai/data/project_live.json"  # Correct path to your JSON file

    print(f"Starting consumer... Watching file {data_file} for new messages.")

    try:
        while True:
            with open(data_file, 'r') as f:
                lines = f.readlines()

            if lines:
                latest_message = json.loads(lines[-1])

                print(f"Reading message: {latest_message}")

                # Process the message
                process_message(json.dumps(latest_message))

            sleep(1)  # Check for new messages every 1 second (adjust as necessary)

    except KeyboardInterrupt:
        print("Consumer interrupted by user.")
    except Exception as e:
        print(f"Error while consuming messages: {e}")
    finally:
        plt.ioff()  # Turn off interactive mode
        plt.show()  # Show the final chart

if __name__ == "__main__":
    main()