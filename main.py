import lasio
import pandas as pd
import os
import collections
import argparse
from pathlib import Path


parser = argparse.ArgumentParser(description='LASFILE MERGER')
parser.add_argument('-f', "--lasfiles_folder_path", type=Path)
args = parser.parse_args()


def dir_path(string):
    if os.path.isdir(string):
        return string
    else:
        raise NotADirectoryError(string)

path = dir_path(args.lasfiles_folder_path)



if not os.path.exists("MergedLAS"):
    os.makedirs("MergedLAS")

las_files = []
df_lists = []
API_Values = []
file_api_zip = []
files_with_api = []
filenames =[]
file_read = []
param_data = []
path = (str(os.getcwd()) + '/' + str(path) + '/').replace("\\","/")

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
    for index, api in enumerate(count):
        files_with_api.append([item for item in file_api_zip if item[1] == count[index]])
    return files_with_api, len(files_with_api) 

"MERGES Parameters"
def append_parameters(parameter):
    parameter = str(parameter)
    parameter += '\n'
    return parameter



lasfile_api , group = lasfile_api_sort(las_files) 
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
        df_lists.append(file_read[index].df())
        df_merge = (pd.concat(df_lists)).sort_index()
        las.set_data(df_merge)
        las.well = file_read[index].well
        param_data.append(append_parameters(file_read[index].params))
        merged_paramteres = '\n#\n'.join(param_data)
        with open(os.getcwd() + '/MergedLAS/' + lasfile_api[num][index][1] + '.las', mode='w') as f:
            las.write(f, version=2.0) 
    with open(os.getcwd() + '/MergedLAS/' + lasfile_api[num][index][1] + '.las','r',encoding='utf-8') as file:
        datas = file.readlines()
    for indice , text in enumerate(datas):
        if text.startswith("~Params"):
            datas[indice + 1] = merged_paramteres
    with open(os.getcwd() + '/MergedLAS/' + lasfile_api[num][index][1] + '.las',mode = 'w') as file:
        file.writelines(datas)
    
    filenames.clear(),file_read.clear(),df_lists.clear(),param_data.clear()

    print ('\n')

