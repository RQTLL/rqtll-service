import sys
import rclpy
from rclpy.node import Node
from sensor_msgs.msg import Image, CompressedImage
from cv_bridge import CvBridge
import cv2

class ImageBridgeNode(Node):
    def __init__(self, topic_name, is_compressed):
        super().__init__('image_bridge_node')
        self.bridge = CvBridge()
        
        self.get_logger().info(f"Subscribing directly to: {topic_name} (compressed={is_compressed})")
        
        if is_compressed:
            self.sub = self.create_subscription(
                CompressedImage,
                topic_name,
                self.compressed_callback,
                10
            )
        else:
            self.sub = self.create_subscription(
                Image,
                topic_name,
                self.raw_callback,
                10
            )

    def raw_callback(self, msg):
        try:
            cv_img = self.bridge.imgmsg_to_cv2(msg, desired_encoding='passthrough')
            
            encoding = msg.encoding.lower()
            
            if len(cv_img.shape) == 3:
                channels = cv_img.shape[2]
                if channels == 3:
                    if 'rgb' in encoding:
                        cv_img = cv2.cvtColor(cv_img, cv2.COLOR_RGB2BGR)
                elif channels == 4:
                    if 'rgba' in encoding:
                        cv_img = cv2.cvtColor(cv_img, cv2.COLOR_RGBA2BGR)
                    else:
                        cv_img = cv2.cvtColor(cv_img, cv2.COLOR_BGRA2BGR)
            elif len(cv_img.shape) == 2:
                pass

            success, encoded_img = cv2.imencode('.jpg', cv_img, [cv2.IMWRITE_JPEG_QUALITY, 80])
            if success:
                jpeg_bytes = encoded_img.tobytes()
                sys.stdout.buffer.write(len(jpeg_bytes).to_bytes(4, 'big'))
                sys.stdout.buffer.write(jpeg_bytes)
                sys.stdout.buffer.flush()
        except Exception as e:
            self.get_logger().error(f"Error in raw_callback: {e}")

    def compressed_callback(self, msg):
        try:
            if msg.format.lower() in ['jpeg', 'jpg']:
                jpeg_bytes = msg.data.tobytes()
            else:
                cv_img = self.bridge.compressed_imgmsg_to_cv2(msg, desired_encoding='passthrough')
                
                if len(cv_img.shape) == 3:
                    if cv_img.shape[2] == 4:
                        cv_img = cv2.cvtColor(cv_img, cv2.COLOR_RGBA2BGR)
                    elif 'rgb' in msg.format.lower():
                        cv_img = cv2.cvtColor(cv_img, cv2.COLOR_RGB2BGR)
                        
                success, encoded_img = cv2.imencode('.jpg', cv_img, [cv2.IMWRITE_JPEG_QUALITY, 80])
                if not success:
                    return
                jpeg_bytes = encoded_img.tobytes()
                
            sys.stdout.buffer.write(len(jpeg_bytes).to_bytes(4, 'big'))
            sys.stdout.buffer.write(jpeg_bytes)
            sys.stdout.buffer.flush()
        except Exception as e:
            self.get_logger().error(f"Error in compressed_callback: {e}")

def main():
    if len(sys.argv) < 3:
        sys.stderr.write("Usage: image_bridge.py <topic_name> <is_compressed_bool>\n")
        sys.exit(1)
        
    topic_name = sys.argv[1]
    is_compressed = sys.argv[2].lower() == 'true'
    
    rclpy.init()
    node = ImageBridgeNode(topic_name, is_compressed)
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        node.destroy_node()
        rclpy.shutdown()

if __name__ == '__main__':
    main()
