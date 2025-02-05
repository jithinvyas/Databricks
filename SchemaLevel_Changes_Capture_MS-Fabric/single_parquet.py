from notebookutils import mssparkutils
import os

out_file_name ='Employee_punchingdata.parquet'		##Output file name 
path = "Files/Gold_zone/"			    	##Output Folder
tmp_path = "Files/temp/Employee_punchdata/"   	

df=emp_data

def copy_file(path,tmp_path,df,out_file_name):
    if mssparkutils.fs.exists(tmp_path):
    # If the folder exists, remove it
        mssparkutils.fs.rm(tmp_path, True)
        # print(f"Folder {folder_path} has been deleted.")
    if not mssparkutils.fs.exists(path):
        mssparkutils.fs.mkdirs(path)

    df.coalesce(1).write.parquet(tmp_path)
    # Path for the source folder
    # source_path = spark._jvm.org.apache.hadoop.fs.Path(tmp_path)
    files = mssparkutils.fs.ls(tmp_path)
    if len(files)>0:
        # Get the last file in the list (no sorting, just the last item in the list)
        last_file = files[-1]
        # print(last_file)
        source_path = last_file.path
        destination_path = os.path.join(path, out_file_name)
        if mssparkutils.fs.exists(destination_path):
            mssparkutils.fs.rm(destination_path, True) #removing existing file
        mssparkutils.fs.mv(source_path, destination_path)# moving latest file
        
copy_file(path,tmp_path,df,out_file_name) 
