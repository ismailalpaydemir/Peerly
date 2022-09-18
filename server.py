import json
import pandas as pd
import warnings
import socket
import threading

warnings.filterwarnings('ignore')


class Server(object):
    def __init__(self, ip, port, size, data="data.csv"):
        self.ip = ip
        self.port = port
        self.size = size
        self.msg_format = "utf-8"
        self.data_path = data

        self.addr = (self.ip, self.port)
        self.data = self.get_data(self.data_path)

        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        if self.ip == "127.0.0.1":
            print(
                "[WARNING] Currently you're using 'LOCALHOST'. To disable that behaviour, you need to change IP address")

    def listen(self):
        self.server.bind(self.addr)
        self.server.listen(5)
        print(f"[READY] Server is listening on {self.ip}:{self.port}")

    @staticmethod
    def get_data(src_data):
        try:
            data = pd.read_csv(src_data, index_col=[0])
        except Exception as err:
            print(err)
            temp_data = [
                "student_id", "IP", "PORT", "TOPIC", "MATCH_STATUS", "MATCHED_STUDENT_ID"
            ]
            data = pd.DataFrame(columns=temp_data)
            data.to_csv(src_data)

        return data

    def update_connection_status(self, src_student_id: int, src_status: int) -> None:
        self.data[self.data["student_id"] == src_student_id] = src_status

    def update_data(self, src_save_dict: dict) -> None:
        if src_save_dict['student_id'] not in self.data['student_id'].tolist():
            self.data = self.data.append(src_save_dict, ignore_index=True)
            self.data.to_csv(self.data_path)
        else:
            print("[LOG] User data has already saved.")

    def get_user_data(self, src_student_id: int) -> dict:
        student_index = self.data[self.data["student_id"] == src_student_id].index
        student_index = student_index[0]
        return self.data.loc[student_index].to_dict()

    def handle_client(self, src_conn, src_addr):
        print(f"[NEW CONNECTION] {src_addr} connected.")
        connected = True

        while connected:
            msg = src_conn.recv(self.size).decode(self.msg_format)
            msg = json.loads(msg)
            if msg["connection"] == 0:
                connected = False

            elif msg["connection"] == -1:
                user_data = self.get_user_data(src_student_id=msg["student_id"])
                back_msg = json.dumps(user_data).encode(self.msg_format)
                src_conn.send(back_msg)

            elif msg["connection"] == 2:
                back_msg = self.data.to_dict()
                back_msg = json.dumps(back_msg).encode(self.msg_format)
                src_conn.send(back_msg)

            else:
                save_dict = {
                    "student_id": msg["student_id"],
                    "IP": src_addr[0],
                    "PORT": src_addr[1],
                    "TOPIC": msg["topic"],
                    "CONNECTION": msg["connection"]
                }
                self.update_data(save_dict)
                res = self.match(save_dict)
                print(f"[{src_addr}] -> {msg}")
                if not res:
                    back_msg = {"status": "received"}
                else:
                    back_msg = {"status": "received", "match": "You get a match! Send 'get' to see it."}
                src_conn.send(json.dumps(back_msg).encode(self.msg_format))

        src_conn.close()

    def match(self, src_dict: dict) -> bool:
        topic = src_dict['TOPIC']
        temp_df = self.data[self.data['TOPIC'] == topic]
        temp_df = temp_df[temp_df['MATCH_STATUS'] != 1]

        if len(temp_df) >= 2:
            first_id = temp_df['student_id'].iloc[0]
            second_id = temp_df['student_id'].iloc[1]

            # Find students indexes
            s_1_loc = self.data[self.data["student_id"] == first_id].index.astype(int)
            s_2_loc = self.data[self.data["student_id"] == second_id].index.astype(int)

            # Update status to one
            self.data.at[s_1_loc[0], "MATCH_STATUS"] = 1
            self.data.at[s_2_loc[0], "MATCH_STATUS"] = 1

            # Enter match names
            self.data.at[s_1_loc[0], "MATCHED_STUDENT_ID"] = second_id
            self.data.at[s_2_loc[0], "MATCHED_STUDENT_ID"] = first_id

            self.data.to_csv(self.data_path)

            return True
        else:
            return False


if __name__ == "__main__":
    IP = "127.0.0.1"
    #IP = "0.0.0.0"
    PORT = 8080
    SIZE = 2048
    FORMAT = "utf-8"

    server = Server(ip=IP, port=PORT, size=SIZE)
    server.listen()
    while True:
        conn, addr = server.server.accept()
        thread = threading.Thread(target=server.handle_client, args=(conn, addr))
        thread.start()
