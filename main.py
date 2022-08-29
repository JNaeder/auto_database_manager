import datetime

import gspread
import mysql.connector
import time


class Spreadsheet:
    def __init__(self):
        self.google_sheet = gspread.oauth()
        self.main_spreadsheet = self.google_sheet.open_by_key('1XgSe-lkrjvOb9y04XGfJ0G2n_nwLSaVFLFd9OxA-KdY')
        self.powerbi_sheet = self.main_spreadsheet.worksheet("NY GPA Dashboard PowerBI")
        self.student_info_sheet = self.main_spreadsheet.worksheet("Student Info")
        self.powerbi_info = self.powerbi_sheet.get_values()[1:]
        self.student_info = self.student_info_sheet.get_values()[1:]

        self.my_spreadsheet = self.google_sheet.open_by_key("17IIW21BzwSirT5g53Un9oYEZUSB0CaRmS55f9ur8n94")
        self.preferred_names = self.my_spreadsheet.worksheet("Preferred Names").get_values()[1:]

    def get_powerbi_values(self):
        return self.powerbi_info

    def get_preferred_names(self):
        return self.preferred_names

    def get_proper_name_from_student_id(self, student_id):
        for data in self.student_info:
            if int(data[1]) == student_id:
                return data[0]


class MySQL:
    def __init__(self):
        self.database = mysql.connector.connect(
            host="192.168.0.191",
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
        icr_num = list_of_data[3][:-1]
        sql = f"INSERT INTO all_active (student_id, student_name, mod_section, icr, gpa, email)" \
              f"VALUES ({list_of_data[1]}, '{list_of_data[2]}', '{list_of_data[6]}'," \
              f" {icr_num}, {list_of_data[4]}, '{list_of_data[1]}.us@saeinstitute.edu');"
        self.cursor.execute(sql)
        self.database.commit()

    def remove_from_table(self, the_id):
        sql = f"DELETE FROM all_active WHERE student_id={the_id}"
        self.cursor.execute(sql)
        self.database.commit()

    def update_student_id_db(self, spreadsheet_info, preferred_name=None):
        new_icr = round(float(spreadsheet_info[3][:-1]), 2)
        new_gpa = round(float(spreadsheet_info[4]), 2)
        proper_name = preferred_name or spreadsheet_info[9]

        sql = f"UPDATE all_active SET " \
              f"student_name='{proper_name}', " \
              f"mod_section='{spreadsheet_info[6]}', " \
              f"icr={new_icr}, " \
              f"gpa={new_gpa} " \
              f"WHERE student_id={spreadsheet_info[1]}"
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

    def is_different_data(self, spreadsheet_data, db_data):
        # Check Name
        preferred_names = self.spreadsheet.get_preferred_names()
        spreadsheet_name = self.spreadsheet.get_proper_name_from_student_id(int(spreadsheet_data[1]))
        for name in preferred_names:
            if name[0] == spreadsheet_name:
                spreadsheet_name = name[1]

        if spreadsheet_name != db_data[1]:
            return True
        # Check Mod
        if spreadsheet_data[6] != db_data[2]:
            return True
        # Check ICR
        if round(float(spreadsheet_data[3][:-1]), 2) != db_data[3]:
            return True
        # Check GPA
        if round(float(spreadsheet_data[4]), 2) != db_data[4]:
            return True
        return False

    def process_data(self):
        list_of_all_db_data = self.mysql.get_all_info_from_database()
        list_of_all_spreadsheet_data = self.spreadsheet.get_powerbi_values()
        list_of_preferred_names = self.spreadsheet.get_preferred_names()

        for index in range(len(list_of_all_spreadsheet_data)):
            data = list_of_all_spreadsheet_data[index]
            if int(data[1]) not in [x[0] for x in list_of_all_db_data]:
                self.mysql.add_row_into_table(data)
                print(f"Added {data[2]} into the table")

        for index in range(len(list_of_all_db_data)):
            data = list_of_all_db_data[index]
            if data[0] not in [int(x[1]) for x in list_of_all_spreadsheet_data]:
                self.mysql.remove_from_table(data[0])
                print(f"Deleted {data[1]} from Table")

        for spreadsheet_data in list_of_all_spreadsheet_data:
            db_data = self.mysql.get_student_info_from_student_id(spreadsheet_data[1])
            if self.is_different_data(spreadsheet_data, db_data):
                proper_name = self.spreadsheet.get_proper_name_from_student_id(int(spreadsheet_data[1]))
                spreadsheet_data.append(proper_name)
                if proper_name in [x[0] for x in list_of_preferred_names]:
                    for name in list_of_preferred_names:
                        if proper_name == name[0]:
                            self.mysql.update_student_id_db(spreadsheet_data, name[1])
                else:
                    self.mysql.update_student_id_db(spreadsheet_data)

                print(f"Changed {spreadsheet_data[2]} Data")


if __name__ == "__main__":
    def main():
        start_time = time.perf_counter()
        controller = MasterController()
        controller.process_data()
        final_time = time.perf_counter() - start_time
        the_datetime = datetime.datetime.now().strftime("%m/%d/%y - %H:%M:%S")
        print(f"{the_datetime} (Finished in {round(final_time, 2)} sec)")


    while True:
        main()
        time.sleep(300)
