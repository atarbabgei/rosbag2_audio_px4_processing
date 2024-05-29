import argparse
import os
import sys
import csv
from rosbags.rosbag2 import Reader
from rosbags.typesys import get_typestore, Stores, get_types_from_msg

# Constants
MOTOR_TOPIC = '/fmu/out/actuator_outputs'  # Motor command topic

# ActuatorOutputs message definition
ACTUATOR_OUTPUTS_MSG = """
uint64 timestamp
uint32 noutputs
float32[16] output
"""

# Initialize the typestore and register the ActuatorOutputs message type
typestore = get_typestore(Stores.ROS2_HUMBLE)
typestore.register(get_types_from_msg(ACTUATOR_OUTPUTS_MSG, 'px4_msgs/msg/ActuatorOutputs'))

ActuatorOutputs = typestore.types['px4_msgs/msg/ActuatorOutputs']

def print_connections(reader):
    """Prints all topic and msgtype information available in the rosbag."""
    for connection in reader.connections:
        print(f"Topic: {connection.topic}, MsgType: {connection.msgtype}")

def process_messages(reader, topic):
    """Processes messages from a specific topic in the rosbag and prints them."""
    connections = [x for x in reader.connections if x.topic == topic]
    for connection, timestamp, rawdata in reader.messages(connections=connections):
        msg = typestore.deserialize_cdr(rawdata, connection.msgtype)
        print(f"Timestamp: {timestamp}, NOutputs: {msg.noutputs}, Output: {msg.output}")

def save_messages_to_csv(reader, topic, csv_filename):
    """Processes messages from a specific topic in the rosbag and saves them to a CSV file."""
    connections = [x for x in reader.connections if x.topic == topic]
    with open(csv_filename, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['Timestamp', 'NOutputs', 'Output'])
        for connection, timestamp, rawdata in reader.messages(connections=connections):
            msg = typestore.deserialize_cdr(rawdata, connection.msgtype)
            writer.writerow([timestamp, msg.noutputs, list(msg.output)])
    print(f"Data saved to {csv_filename}")

def main():
    """Main function to read the rosbag, process messages, and save them to a CSV file."""
    parser = argparse.ArgumentParser(description='Parse and save ActuatorOutputs messages from a rosbag.')
    parser.add_argument('--dir', type=str, required=True, help='Path to the rosbag file')

    args = parser.parse_args()
    bag_path = args.dir

    if not os.path.exists(bag_path):
        print(f"Error: The file '{bag_path}' does not exist.")
        sys.exit(1)

    output_file = os.path.splitext(os.path.basename(bag_path))[0] + '_motor_commands.csv'

    with Reader(bag_path) as reader:
        print("Connections:")
        print_connections(reader)
        
        print("\nMotor Commands:")
        process_messages(reader, MOTOR_TOPIC)
        
        print("\nSaving Motor Commands to CSV...")
        save_messages_to_csv(reader, MOTOR_TOPIC, output_file)

if __name__ == "__main__":
    main()
