import rclpy
from rclpy.node import Node
from sensor_msgs.msg import Image
from std_msgs.msg import Header
from rcl_interfaces.msg import SetParametersResult
from cv_bridge import CvBridge
import cv2
import time

import sys
import os

sys.path.append(
    os.path.dirname(
        os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
    )
)
# 引入你的 Camera 类
from robonix.capability.orbc_gemini215.api.camera_api import Camera


class CameraPublisher(Node):
    def __init__(self):
        super().__init__("camera_publisher")

        self.camera_name = "camera"

        # 发布主题
        topic = f"/{self.camera_name}/camera/color/image_raw"
        self.publisher_ = self.create_publisher(Image, topic, 10)
        self.get_logger().info(f"Publishing color image on: {topic}")

        # 初始化 Camera 类，只取彩色
        self.camera = Camera(color=True, depth=False)
        self.bridge = CvBridge()

        # 定时器，控制发布频率（1 FPS）
        self.timer = self.create_timer(1.0 / 1.0, self.publish_frame)

        # 支持运行时修改 camera_name 参数
        self.add_on_set_parameters_callback(self.on_set_parameters)

    def on_set_parameters(self, params):
        for p in params:
            if p.name == "camera_name" and p.type == p.TYPE_STRING:
                self.camera_name = p.value
                topic = f"/{self.camera_name}/camera/color/image_raw"
                # 重新绑定发布器
                self.publisher_ = self.create_publisher(Image, topic, 10)
                self.get_logger().info(f"Switched publishing topic to: {topic}")
        return SetParametersResult(successful=True)

    def publish_frame(self):
        try:
            frames = self.camera.get_frames()
            frame_rgb = frames.get("color", None)

            if frame_rgb is None:
                # 若无图像，略过本次循环
                return

            # 转换为 ROS Image
            msg = self.bridge.cv2_to_imgmsg(frame_rgb, encoding="bgr8")
            # 填充标准头
            msg.header = Header()
            msg.header.stamp = self.get_clock().now().to_msg()
            msg.header.frame_id = f"{self.camera_name}_color_optical_frame"

            self.publisher_.publish(msg)
        except Exception as e:
            self.get_logger().error(f"Failed to publish frame: {e}")

    def destroy_node(self):
        try:
            if hasattr(self, "camera") and self.camera:
                self.camera.close()
        except Exception:
            pass
        super().destroy_node()


def main(args=None):
    rclpy.init(args=args)
    node = CameraPublisher()
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        node.destroy_node()
        rclpy.shutdown()


if __name__ == "__main__":
    main()
