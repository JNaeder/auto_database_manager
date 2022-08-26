import gspread
import mysql.connector
import time


class Spreadsheet:
    def __init__(self):
        self.google_sheet = gspread.oauth()
        self.main_spreadsheet = self.google_sheet.open_by_key('1cwRxSt-tGRmwghuPMzS2DMNMjOpG9xElNuKrEtt0Eco')
        self.sheet = self.main_spreadsheet.worksheet("All Active")
        self.all_values = self.sheet.get_values()[1:]

    def get_all_values(self):
        return self.all_values


class MySQL:
    def __init__(self):
        self.database = mysql.connector.connect(
            host="192.168.0.192",
            # host="2.tcp.ngrok.io",
            # port=16493,
            user="johnny",
            password="password",
            database="test_database"
        )
        self.cursor = self.database.cursor()

    def get_all_info_from_database(self):
        sql = f"SELECT * FROM all_active;"
        self.cursor.execute(sql)
        return self.cursor.fetchall()

    def add_row_into_table(self, list_of_data):
        sql = f"INSERT INTO all_active (student_id, student_name, mod_section, icr, gpa, email, instructor)" \
              f"VALUES ({list_of_data[0]}, '{list_of_data[1]}', '{list_of_data[2]}'," \
              f" {list_of_data[5]}, {list_of_data[6]}, '{list_of_data[3]}', '{list_of_data[4]}');"
        self.cursor.execute(sql)
        self.database.commit()

    def remove_from_table(self, the_id):
        sql = f"DELETE FROM all_active WHERE student_id={the_id}"
        self.cursor.execute(sql)
        self.database.commit()

    def update_student_id_db(self, spreadsheet_info):
        sql = f"UPDATE all_active SET " \
              f"student_name='{spreadsheet_info[1]}', " \
              f"mod_section='{spreadsheet_info[2]}', " \
              f"icr={spreadsheet_info[5]}, " \
              f"gpa={spreadsheet_info[6]}, " \
              f"instructor='{spreadsheet_info[4]}' " \
              f"WHERE student_id={spreadsheet_info[0]}"
        self.cursor.execute(sql)
        self.database.commit()

    def get_student_info_from_student_id(self, student_id):
        sql = f"SELECT * FROM all_active WHERE student_id={student_id}"
        self.cursor.execute(sql)
        return self.cursor.fetchone()


class MasterController:
    def __init__(self):
        self.spreadsheet = Spreadsheet()
        self.mysql = MySQL()

    @staticmethod
    def is_different_data(spreadsheet_data, db_data):
        if spreadsheet_data[1] != db_data[1]:
            return True
        if spreadsheet_data[2] != db_data[2]:
            return True
        if float(spreadsheet_data[5]) != db_data[3]:
            return True
        if float(spreadsheet_data[6]) != db_data[4]:
            return True
        if spreadsheet_data[4] != db_data[6]:
            return True
        return False

    def process_data(self):
        list_of_all_db_data = self.mysql.get_all_info_from_database()
        list_of_all_spreadsheet_data = self.spreadsheet.get_all_values()

        for index in range(len(list_of_all_spreadsheet_data)):
            data = list_of_all_spreadsheet_data[index]
            if int(data[0]) not in [x[0] for x in list_of_all_db_data]:
                self.mysql.add_row_into_table(data)
                print(f"Added {data[1]} into the table")

        for index in range(len(list_of_all_db_data)):
            data = list_of_all_db_data[index]
            if data[0] not in [int(x[0]) for x in list_of_all_spreadsheet_data]:
                self.mysql.remove_from_table(data[0])
                print(f"Deleted {data[1]} from Table")

        for spreadsheet_data in list_of_all_spreadsheet_data:
            db_data = self.mysql.get_student_info_from_student_id(spreadsheet_data[0])
            if self.is_different_data(spreadsheet_data, db_data):
                self.mysql.update_student_id_db(spreadsheet_data)
                print(f"Changed {spreadsheet_data[1]} Data")


if __name__ == "__main__":
    def main():
        start_time = time.perf_counter()
        controller = MasterController()
        controller.process_data()
        final_time = time.perf_counter() - start_time
        print(f"Done in {final_time} seconds")

    while True:
        main()
        time.sleep(30)
