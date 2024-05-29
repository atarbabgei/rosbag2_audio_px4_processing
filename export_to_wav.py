'''
Description: This script reads audio data from a rosbag file and saves it to a .wav file.

Dependencies:
- FFmpeg: Required for exporting audio data to a .wav file.
- Python packages: rosbags, pydub

'sudo apt-get install ffmpeg'
'pip3 install rosbags pydub'

Usage: python3 export_to_wav.py --dir /path/to/your/rosbag/folder
Example: python3 export_to_wav.py --dir ~/Documents/rosbags/rosbag2_2024_05_29-15_46_13

Assumption: The audio data is published under the topic /audio/audio using the message type audio_common_msgs/msg/AudioData.
'''

import argparse
import os
import sys
from rosbags.rosbag2 import Reader
from rosbags.typesys import get_typestore, Stores, get_types_from_idl
from pydub import AudioSegment

# Constants
AUDIO_TOPIC = '/audio/audio'  # Audio topic

# AudioData message definition (example definition, adjust according to your actual message definition)
AUDIO_DATA_MSG = """
module audio_common_msgs {
  module msg {
    struct AudioData {
      sequence<uint8> data;
    };
  };
};
"""

# Initialize the typestore and register the AudioData message type
typestore = get_typestore(Stores.ROS2_HUMBLE)
typestore.register(get_types_from_idl(AUDIO_DATA_MSG))

def print_connections(reader):
    """Prints all topic and msgtype information available in the rosbag."""
    for connection in reader.connections:
        print(connection.topic, connection.msgtype)

def collect_audio_data(reader, topic):
    """Collects audio data from the specified topic in the rosbag and returns it as a single byte array."""
    audio_data = bytearray()
    
    # Find the connection for the specified topic
    connections = [x for x in reader.connections if x.topic == topic]
    for connection, timestamp, rawdata in reader.messages(connections=connections):
        msg = typestore.deserialize_cdr(rawdata, connection.msgtype)
        audio_data.extend(msg.data)
    
    return audio_data

def save_audio_to_file(audio_data, output_file):
    """Saves the collected audio data to a .wav file."""
    audio_segment = AudioSegment(
        data=bytes(audio_data),
        sample_width=2,  # Assuming 16-bit samples (2 bytes per sample)
        frame_rate=44100,  # Assuming a sample rate of 44100 Hz
        channels=1  # Assuming mono audio
    )
    audio_segment.export(output_file, format='wav')
    print(f"Audio data saved to {output_file}")

def main():
    """Main function to read the rosbag, collect audio data, and save it to a file."""
    parser = argparse.ArgumentParser(description='Read audio data from a rosbag and save it to a .wav file.')
    parser.add_argument('--dir', type=str, required=True, help='Path to the rosbag file')

    args = parser.parse_args()
    bag_path = args.dir

    if not os.path.exists(bag_path):
        print(f"Error: The file '{bag_path}' does not exist.")
        sys.exit(1)

    output_file = os.path.splitext(os.path.basename(bag_path))[0] + '.wav'

    with Reader(bag_path) as reader:
        print("Reading audio data from the bag file...")
        audio_data = collect_audio_data(reader, AUDIO_TOPIC)
        
        print("Saving audio data to file...")
        save_audio_to_file(audio_data, output_file)

if __name__ == "__main__":
    main()
