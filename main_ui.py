# -*- coding: utf-8 -*-

# Form implementation generated from reading ui file 'lasmerger.ui'
#
# Created by: PyQt5 UI code generator 5.15.7
#
# WARNING: Any manual changes made to this file will be lost when pyuic5 is
# run again.  Do not edit this file unless you know what you are doing.

import os 
from PyQt5 import QtCore, QtGui, QtWidgets
from PyQt5.QtCore import (Qt, pyqtSignal,QObject)

from PyQt5.QtWidgets import *
import lasio
import pandas as pd
import os
import collections
import argparse
from pathlib import Path
import shutil
default_path = os.path.dirname(os.path.abspath(__file__))

class Stream(QObject):
    newText = pyqtSignal(str)

    def write(self, text):
        self.newText.emit(str(text))

class Ui_LasMerger(object):
    def setupUi(self, LasMerger):
        LasMerger.setObjectName("LasMerger")
        LasMerger.resize(2800, 2000)
        self.centralwidget = QtWidgets.QWidget(LasMerger)
        self.centralwidget.setObjectName("centralwidget")
        self.pushButton = QtWidgets.QPushButton(self.centralwidget)
        self.pushButton.setGeometry(QtCore.QRect(200, 80, 250, 60))
        self.pushButton.setObjectName("pushButton")
        self.pushButton_3 = QtWidgets.QPushButton(self.centralwidget)
        self.pushButton_3.setGeometry(QtCore.QRect(2100, 1800, 120, 60))
        self.pushButton_3.setObjectName("pushButton_3")
        self.lineEdit = QtWidgets.QLineEdit(self.centralwidget)
        self.lineEdit.setGeometry(QtCore.QRect(200, 160, 750, 60))
        self.lineEdit.setObjectName("lineEdit")
        self.plainTextEdit = QtWidgets.QPlainTextEdit(self.centralwidget)
        self.plainTextEdit.setGeometry(QtCore.QRect(1500, 20, 1200, 1700))
        self.plainTextEdit.setObjectName("plainTextEdit")
        LasMerger.setCentralWidget(self.centralwidget)
        self.menubar = QtWidgets.QMenuBar(LasMerger)
        self.menubar.setGeometry(QtCore.QRect(0, 0, 1200, 20))
        self.menubar.setObjectName("menubar")
        LasMerger.setMenuBar(self.menubar)
        self.statusbar = QtWidgets.QStatusBar(LasMerger)
        self.statusbar.setObjectName("statusbar")
        LasMerger.setStatusBar(self.statusbar)
        self.pushButton.clicked.connect(self.openFileNameDialog)
        self.pushButton_3.clicked.connect(self.run_script)

        self.retranslateUi(LasMerger)
        QtCore.QMetaObject.connectSlotsByName(LasMerger)


        sys.stdout = Stream(newText=self.onUpdateText)
        self.plainTextEdit.moveCursor(QtGui.QTextCursor.Start)
        self.plainTextEdit.ensureCursorVisible()

    def onUpdateText(self, text):
        cursor = self.plainTextEdit.textCursor()
        cursor.movePosition(QtGui.QTextCursor.End)
        cursor.insertText(text)
        self.plainTextEdit.setTextCursor(cursor)
        self.plainTextEdit.ensureCursorVisible()

    def retranslateUi(self, LasMerger):
        _translate = QtCore.QCoreApplication.translate
        LasMerger.setWindowTitle(_translate("LasMerger", "MainWindow"))
        self.pushButton.setText(_translate("LasMerger", "Input Directory"))
        self.pushButton_3.setText(_translate("LasMerger", "Run"))
    def openFileNameDialog(self):
        self.lineEdit.setText(QFileDialog.getExistingDirectory(None, "Select Las Directory" ,default_path))
        self.directory_path = str(self.lineEdit.text())
    def dir_path(self,string):
        if os.path.isdir(string):
            return string
        else:
            raise NotADirectoryError(string)

    def run_script(self):

        las_files = []
        df_lists = []
        API_Values = []
        file_api_zip = []
        files_with_api = []
        filenames =[]
        file_read = []
        param_data = []
        one_log_files = []    

        path = self.dir_path(str(self.directory_path))

        if not os.path.exists("MergedLAS"):
            os.makedirs("MergedLAS")


        path = (str(path) + '/').replace("\\","/")


        "LIST LAS FILES from LAS-FOLDER"
        def las_files_list(path):
            files = os.listdir(path)
            for file in files:
                if file.lower().endswith('.las'):
                    las_files.append(path + file)
            return las_files
        
            
        las_files = las_files_list(path)
        "Zips and Sorts Lasfiles based on their API Numbers"
        def lasfile_api_sort(las_file):
            for idx , lasfile in enumerate(las_files):
                las = lasio.read(lasfile)
                API_Values.append(las.well.API.value)
                file_api_zip.append((tuple([lasfile,API_Values[idx]])))
            count = [item for item, count in collections.Counter(API_Values).items() if count > 1]
            non_count = [item for item, count in collections.Counter(API_Values).items() if count == 1]
            for index, api in enumerate(count):
                files_with_api.append([item for item in file_api_zip if item[1] == count[index]])
            for index, api in enumerate(non_count):
                one_log_files.append([item for item in file_api_zip if item[1] == non_count[index]])
            
            return files_with_api, one_log_files ,len(files_with_api) 

        "MERGES Parameters"
        def append_parameters(parameter):
            parameter = str(parameter)
            parameter += '\n'
            return parameter


        def parameter_splicer(lasfile):
            with open(str(lasfile).replace("\\","/"),'r',encoding='utf-8') as file:
                datas = file.readlines()

            for indice , text in enumerate(datas):
                if text.startswith("~Params"):
                    param_start = indice + 1
                if text.startswith("~Curve"):
                    curve_start = indice
            params = ""
            for i in range(param_start,curve_start):
                params += datas[i]
            return params

        def curve_point_remover(merged_las_file):
            with open(str(merged_las_file).replace("\\","/"),mode = 'r') as file:
                datas = file.readlines()
            for indice , text in enumerate(datas):
                if text.startswith("~Curve"):
                    curve_start = indice + 1
                if text.startswith("~ASCII"):
                    ascii_start = indice
            for i in range(curve_start,ascii_start):
                datas[i] = ''.join(datas[i].rsplit('.', 1))
            with open(str(merged_las_file).replace("\\","/"),mode = 'w') as file:
                file.writelines(datas)

        def las_to_df_with_units(las):
            column_mapper = {
                c.mnemonic: f"{c.mnemonic}{'.' + c.unit}"
                for c in las.curves
            }
            return (
                las.df()
                .reset_index()
                .rename(columns=column_mapper)
                .set_index(list(column_mapper.values())[0])
            )

        lasfile_api , one_log_file ,group = lasfile_api_sort(las_files) 

        print ("TOTAL Number of LASFILES:" , len(las_files))   
        print ('\n') 
        for num in range(group):
            lasfiles_group = len(lasfile_api[num])
            print ("Number of LASFILES to MERGE:" , lasfiles_group)
            print ("API:" , lasfile_api[num][0][1])
            for index,files in enumerate(lasfile_api[num]):        
                print ("\t LASFILE_" + str(index + 1) + ':' , lasfile_api[num][index][0])
                filenames.append(lasfile_api[num][index][0])
                "WELL LOG DEPTH ADJUST"
                file_read.append(lasio.read(filenames[index]))
                las = lasio.LASFile()
                df_lists.append(las_to_df_with_units(file_read[index]))
                df_merge = (pd.concat(df_lists)).sort_index()
                # df_merge = df_merge.drop_duplicates()
                df_merge = df_merge.groupby(df_merge.index).max()

                las.set_data(df_merge)

                las.well = file_read[index].well
                param_data.append(parameter_splicer(filenames[index]))
                merged_paramteres = '#\n'.join(param_data)        
                with open(os.getcwd() + '/MergedLAS/' + lasfile_api[num][index][1] + '.las', mode='w') as f:
                    las.write(f, version=2.0) 
            with open(os.getcwd() + '/MergedLAS/' + lasfile_api[num][index][1] + '.las','r',encoding='utf-8') as file:
                datas = file.readlines()
            for indice , text in enumerate(datas):
                if text.startswith("~Params"): 
                    datas[indice + 1] = merged_paramteres
            with open(os.getcwd() + '/MergedLAS/' + lasfile_api[num][index][1] + '.las',mode = 'w') as file:
                file.writelines(datas)
            curve_point_remover(os.getcwd() + '/MergedLAS/' + lasfile_api[num][index][1] + '.las')
            filenames.clear(),file_read.clear(),df_lists.clear(),param_data.clear()
            print ('\n')
        "ONE LOG FILE - API NAMING"
        for num in range(len(one_log_file)):
            lasfiles = len(one_log_file[num])
            print ("Number of ONELOG - LASFILES to MERGE:" , lasfiles)
            print ("API:" , one_log_file[num][0][1])
            for index,files in enumerate(one_log_file[num]):        
                print ("\t LASFILE_" + str(index + 1) + ':' , one_log_file[num][index][0]) 
                filenames.append(one_log_file[num][index][0])
                with open(os.getcwd() + '/MergedLAS/' + one_log_file[num][index][1] + '.las', mode='w') as f:
                    las.write(f, version=2.0) 

                source = one_log_file[num][index][0]
                target = os.getcwd() + '/MergedLAS/' + one_log_file[num][index][1] + '.las'
                shutil.copy(source, target)
            filenames.clear()
            print ('\n')



if __name__ == "__main__":
    import sys
    app = QtWidgets.QApplication(sys.argv)
    AdminDashBoard = QtWidgets.QMainWindow()
    ui = Ui_LasMerger()
    ui.setupUi(AdminDashBoard)
    AdminDashBoard.show()
    sys.exit(app.exec_())

