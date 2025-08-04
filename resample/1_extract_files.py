import os
import shutil

# Path where all your HISTDATA_COM_ASCII_EURUSD_M1YYYY folders are located
base_path = r"C:\Users\zak\Desktop\workspace\datalake\raw\eurusd"

# Destination path (root folder where you want the renamed files)
dest_path = base_path  # change if you want them somewhere else

for folder in os.listdir(base_path):
    folder_path = os.path.join(base_path, folder)

    # Only process folders that match the pattern
    if os.path.isdir(folder_path) and folder.startswith("HISTDATA_COM_ASCII_EURUSD_M1"):
        # Extract year from folder name
        year = folder[-4:]

        # Find the original file
        src_file = os.path.join(folder_path, f"DAT_ASCII_EURUSD_M1_{year}.csv")

        if os.path.exists(src_file):
            # New file name
            dst_file = os.path.join(dest_path, f"EURUSD_M1_{year}.csv")

            # Copy & rename
            shutil.copy2(src_file, dst_file)
            print(f"Copied {src_file} → {dst_file}")
        else:
            print(f"File not found in {folder_path}")

print("✅ All files copied and renamed.")
