import socket
import json
import pandas as pd


def get_project_topics_api(src_dst) -> dict:
    with open(src_dst, "r") as js:
        out = json.loads(js.read())
    return out


class Client(object):
    def __init__(self, ip, port, size):
        self.ip = ip
        self.port = port
        self.size = size
        self.msg_format = "utf-8"
        self.addr = (self.ip, self.port)

        self.client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        if self.ip == "127.0.0.1":
            print(
                "[WARNING] Currently you're using 'LOCALHOST'. To disable that behaviour, you need to change IP address")

        self.user_data = self.get_user_choice()

    @staticmethod
    def get_user_choice():
        student_id = int(input("Enter Student ID -> "))
        topic_number = int(input("Enter Topic -> "))
        return {
            "student_id": student_id,
            "topic": topic_number,
            "connection": 1
        }

    def send(self):
        self.client.connect(self.addr)
        print(f"[CONNECTED] Client connected to server at {self.ip}:{self.port}")
        connected = True
        while connected:

            progress = input("Command -> ")
            if progress == "send":
                msg = json.dumps(self.user_data).encode(self.msg_format)
                self.client.send(msg)

                echo = self.client.recv(self.size).decode(self.msg_format)
                recv_msg = json.loads(echo)
                if recv_msg["status"] == 1:
                    connected = False
                else:
                    print(f"[SERVER] {recv_msg}")

            elif progress == "all":
                self.user_data['connection'] = 2
                msg = json.dumps(self.user_data).encode(self.msg_format)
                self.client.send(msg)

                # get server response
                echo = self.client.recv(self.size).decode(self.msg_format)
                recv_msg = json.loads(echo)
                print(f"ALL DATA: \n{pd.DataFrame.from_dict(recv_msg)}")
            elif progress == "get":
                # send server to get msg and listen from server to result
                self.user_data['connection'] = -1
                msg = json.dumps(self.user_data).encode(self.msg_format)
                self.client.send(msg)

                # get server response
                echo = self.client.recv(self.size).decode(self.msg_format)
                recv_msg = json.loads(echo)
                print(f"[CURRENT RESULT] -> {recv_msg}")

            else:
                self.user_data['connection'] = 0
                msg = json.dumps(self.user_data).encode(self.msg_format)
                self.client.send(msg)
                connected = False


if __name__ == "__main__":
    IP = "127.0.0.1"
    PORT = 8080
    SIZE = 2048
    FORMAT = "utf-8"
    print(f"[WELCOME] Current project topics are:\n"
          f"{get_project_topics_api('project_topics.json')}")

    client = Client(ip=IP, port=PORT, size=SIZE)
    client.send()
