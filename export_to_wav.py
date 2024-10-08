'''
Description: This script reads audio data from a rosbag file and saves it to a .wav file, and generates a .txt file with audio metadata.

Dependencies:
- Python packages: rosbags, scipy

Installation:
'pip3 install rosbags scipy'

Usage: python3 export_to_wav.py --dir /path/to/your/rosbag/folder
Example: python3 export_to_wav.py --dir ~/Documents/rosbags/rosbag2_2024_05_29-15_46_13 

Assumption: The audio data is published under the topic /aaudio_stream using the message type audio_msgs/msg/Audio.
'''

import argparse
import os
import sys
import numpy as np
import scipy.io.wavfile as wav
from rosbags.rosbag2 import Reader
from rosbags.typesys import get_typestore, Stores, get_types_from_idl
from datetime import datetime

# Constants
AUDIO_TOPIC = '/audio_stream'  # Adjust this to your actual topic if needed

# Audio message definition based on the custom message
AUDIO_MSG = """
module audio_msgs {
  module msg {
    struct Audio {
      uint64 timestamp;
      int32 sample_rate;
      int32 channels;
      sequence<float> data;
    };
  };
};
"""

# Initialize the typestore and register the Audio message type
typestore = get_typestore(Stores.ROS2_HUMBLE)
typestore.register(get_types_from_idl(AUDIO_MSG))

def print_connections(reader):
    """Prints all topic and msgtype information available in the rosbag."""
    for connection in reader.connections:
        print(connection.topic, connection.msgtype)

def collect_audio_data(reader, topic):
    """Collects audio data from the specified topic in the rosbag and returns it as a numpy array and metadata."""
    audio_buffer = []
    first_timestamp = None
    sample_rate = None
    channels = None

    # Find the connection for the specified topic
    connections = [x for x in reader.connections if x.topic == topic]
    for connection, timestamp, rawdata in reader.messages(connections=connections):
        if first_timestamp is None:
            first_timestamp = timestamp
        msg = typestore.deserialize_cdr(rawdata, connection.msgtype)

        # Update sample rate and channels from the message
        if sample_rate is None:
            sample_rate = msg.sample_rate
        if channels is None:
            channels = msg.channels

        # Append audio data
        audio_data = np.array(msg.data, dtype=np.float32).reshape(-1, channels)
        audio_buffer.append(audio_data)

    # Concatenate the buffered audio data into a single numpy array
    audio_data_np = np.concatenate(audio_buffer, axis=0)

    metadata = {
        "First Timestamp": first_timestamp,
        "Sample Rate": sample_rate,
        "Channels": channels,
    }

    return audio_data_np, metadata

def save_audio_to_file(audio_data, output_file, sample_rate, channels):
    """Saves the collected audio data to a .wav file."""
    wav.write(output_file, sample_rate, audio_data)
    print(f"Audio data saved to {output_file}")

def save_metadata_to_file(metadata, output_file):
    """Saves the metadata information to a .txt file."""
    with open(output_file, 'w') as file:
        for key, value in metadata.items():
            file.write(f"{key}: {value}\n")
    print(f"Metadata saved to {output_file}")

def main():
    """Main function to read the rosbag, collect audio data, and save it to a file."""
    parser = argparse.ArgumentParser(description='Read audio data from a rosbag and save it to a .wav file.')
    parser.add_argument('--dir', type=str, required=True, help='Path to the rosbag file')
    args = parser.parse_args()

    bag_path = args.dir

    if not os.path.exists(bag_path):
        print(f"Error: The file '{bag_path}' does not exist.")
        sys.exit(1)

    # Generate output file names based on the current timestamp
    current_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    output_wav_file = f"audio_recording_{current_time}.wav"
    metadata_file = f"audio_metadata_{current_time}.txt"

    # Read from the rosbag
    with Reader(bag_path) as reader:
        print("Reading audio data from the bag file...")
        audio_data, metadata = collect_audio_data(reader, AUDIO_TOPIC)

        print("Saving audio data to file...")
        save_audio_to_file(audio_data, output_wav_file, metadata['Sample Rate'], metadata['Channels'])

        print("Saving metadata to file...")
        save_metadata_to_file(metadata, metadata_file)

if __name__ == "__main__":
    main()
