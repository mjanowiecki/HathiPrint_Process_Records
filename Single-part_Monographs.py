import tarfile
import xml.etree.ElementTree as ET
import pandas as pd
import glob
import sys
import datetime
import csv
import re


def unzip_tar(zip_folder_name, new_folder_name):
	temp = glob.glob(zip_folder_name + "/*.tar.gz")
	old_path = zip_folder_name + "\\"
	new_path = zip_folder_name + "/"
	all_zip = [i.replace(old_path, new_path) for i in temp]
	xml_name_list = []
	for i in range(len(all_zip)):
		name1 = all_zip[i]
		name2 = all_zip[i].lstrip(new_path).rstrip(".tar.gz") + ".xml"
		my_tar = tarfile.open(name1)
		my_tar.extract(name2, new_folder_name + "/")
		my_tar.close()
		xml_name_list.append(new_folder_name + "/" + name2)
	return xml_name_list


def read_one_xml(file_name):
	oclc_prefixes = ['ocm', 'ocn', 'on', '(OCoLC)']
	record_list = []
	root = ET.parse(file_name).getroot()
	print("Reading {} xml records.".format(len(root)))
	for record in root.findall("./record"):
		mms_id = record.find("./controlfield[@tag='001']").text
		gov = record.find("./datafield[@tag='086']")
		if gov:
			gov_ind = 1
		else:
			gov_ind = 0
		oclc_list = []
		bad = False
		if record.findall("./datafield[@tag='035'].subfield[@code='a']"):
			for valid_re in record.findall("./datafield[@tag='035'].subfield[@code='a']"):
				if valid_re.text:
					oclc = valid_re.text
					oclc = oclc.strip()
					oclc = oclc.strip("\\")
					for prefix in oclc_prefixes:
						if prefix in oclc:
							oclc = oclc.replace('(OCoLC)', '')
							oclc = oclc.replace(prefix, '')
							match = re.search(r'\D', oclc)
							if match:
								bad = True
							if len(oclc) < 8:
								oclc = oclc.zfill(8)
								oclc = '(OCoLC)' + oclc
							else:
								oclc = '(OCoLC)' + oclc
							if oclc not in oclc_list:
								oclc_list.append(oclc)
		if record.findall("./datafield[@tag='977']") is not None:
			for item in record.findall("./datafield[@tag='977']"):
				if item.find("./subfield[@code='g']") is not None:
					sta_text = item.find("./subfield[@code='g']").text.lower()
					if sta_text and "withdrawn" in sta_text:
						sta = "WD"
					elif sta_text and ("missing" in sta_text or "lost" in sta_text):
						sta = "LM"
					else:
						sta = "CH"
				else:
					sta = "CH"
		else:
			sta = "CH"
		oclc = ','.join(oclc_list)
		l_re = [oclc, mms_id, sta, gov_ind, bad]
		record_list.append(l_re)
	return record_list


def read_all_files(xml_name_list, result_folder_name):
	dt = datetime.datetime.now()
	dt = dt.strftime("%Y%m%d")
	c_name = ["oclc", "local_id", "status", "govdoc", "bad_oclc"]
	df = pd.DataFrame(columns=c_name)
	total_files = len(xml_name_list)
	for i in xml_name_list:
		print('Now processing {} of {} xml files.'.format(i, total_files))
		rv_d = read_one_xml(i)
		new_df = pd.DataFrame(rv_d, columns=c_name)
		df = pd.concat([df, new_df], ignore_index=True, sort=True)
	print("{} records collected in DataFrame without de-duplication.".format(len(df)))
	df.drop_duplicates(inplace=True)
	print("{} records collected in DataFrame after de-duplication.".format(len(df)))
	result_filename = 'jhu_mono_full_'+dt+'.tsv'
	df.to_csv(result_folder_name+"/"+result_filename, sep='\t', index=False, quoting=csv.QUOTE_ALL)
	print('Creating result file named {}'.format(result_filename))
	return


def go(zip_folder_name, new_folder_name, result_folder_name):
	xml_name_list = unzip_tar(zip_folder_name, new_folder_name)
	print("Finish unzipping tar folder(s).")
	print("Start parsing .xml files.")
	read_all_files(xml_name_list, result_folder_name)
	print("Finished!")


if __name__ == "__main__":
	num_args = len(sys.argv)
	if num_args < 4:
		print("Error: please read ReadMe file carefully about how to run the script.")
	else:
		go(sys.argv[1], sys.argv[2], sys.argv[3])
