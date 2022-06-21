from logging import exception
import time
import datetime
import pathlib
import requests
import json
import pandas as pd
import os
import pendulum
import pickle
from pandas import json_normalize

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable
from airflow.exceptions import AirflowSkipException, AirflowWebServerTimeout, AirflowBadRequest, \
	AirflowNotFoundException, AirflowException, AirflowFailException
from airflow.sensors.python import PythonSensor
from airflow.utils.task_group import TaskGroup


def connect_db():
	connect = False
	num_attempts = 5
	while not connect:
		try:
			print('Попытка подключения к postgress')
			pg_hook = PostgresHook(postgres_conn_id="yandex_direct_airflow")
			connection = pg_hook.get_conn()
			cursor = connection.cursor()
		except Exception as err:
			print(err)
			if num_attempts != 0:
				time.sleep(10)
				num_attempts -= 1
			else:
				raise AirflowSkipException("Can't connect to database")
		else:
			connect = True
	return connection, cursor


def get_conf():
	# Variable
	token = Variable.get("direct_token_pass")
	time_var = dict(Variable.get('yandex_direct', deserialize_json=True))
	url = time_var['url']
	headers = time_var['headers']
	headers['Authorization'] = f'Bearer {token}'
	p = '/opt/airflow/local_volume/yandex_direct'
	#    start_date = pendulum.datetime(2021, 1, 1)
	return url, headers, p


def request_error(log):
	if log.get('error', None) is None:
		return log
	else:
		error_code = log.get('error').get('error_code')
		error_string = log.get('error').get('error_string')
		raise AirflowBadRequest(f'Ошибка: {error_code}, описание: {error_string}')


def get_load_date(**kwargs):
	connection, cursor = connect_db()
	sql = 'select dt from public."ServerTime"'
	cursor.execute(sql)
	rez = cursor.fetchall()
	cursor.close()
	connection.close()
	return rez


def change_query(special_dict):
	sql_string = str()
	if 'primary_key' in special_dict.keys():
		sql_string += f",{special_dict.get('primary_key')}"
	if 'foreign_key' in special_dict.keys():
		sql_string += f",{special_dict.get('foreign_key')}"
	return sql_string


def make_dict_df(s_tables):
	d_df = dict()
	""" Удалим dict 'special_data' из данных таблицы """
	for sql_key in s_tables.keys():
		t_dict = s_tables.get(sql_key).copy()
		if "special_data" in s_tables.get(sql_key):
			t_dict.pop('special_data')
		if "PRIMARY KEY" in s_tables.get(sql_key):
			t_dict.pop('PRIMARY KEY')
		time_df = pd.DataFrame(data=None, columns=list(t_dict.keys()))
		d_df[sql_key] = time_df
	return d_df


def write_data_to_postgres(dict_df):
	connection, cursor = connect_db()
	for table in dict_df.keys():
		df = dict_df.get(table)
		if len(df) == 0:
			continue
		tuples = [tuple(x) for x in df.to_numpy()]
		time_list = []
		for num in df:
			time_list.append(f'"{num}"')
		cols = ','.join(time_list)
		query = f'INSERT INTO "{table}"({cols}) VALUES({"%s," * (df.values.shape[1] - 1)}{"%s" * 1})'
		try:
			cursor.executemany(query, tuples)
			print('FINISH QUERY:', query)
		except Exception as err:
			print(err)
			raise AirflowFailException(f"Can't write {table} to database")
	else:
		cursor.close()
		connection.commit()
		connection.close()


def add_data_df(server_time, time_td, t_df, campaigns_dict, table_name, type_data, sec_name, dict_dataframe):
	add_data = time_td.get('special_data').get('add_data')
	for key, v in add_data.items():
		if key == 'ServerDateTime':
			t_df[key] = server_time
		else:
			t_df[key] = campaigns_dict.get(v)
	if type_data != table_name:
		table_name = f'{type_data}.{table_name}'
	if sec_name is not False:
		table_name = f'{table_name}.{sec_name}'
	old_df = dict_dataframe.get(table_name)
	dict_dataframe[table_name] = pd.concat([old_df, t_df], ignore_index=True)
	return dict_dataframe


def add_df(server_time, type_data, table_name, campaigns_dict, sql_tables_dict, dict_dataframe, deep=False,
		   sec_name=False, have_deep_key=False):
	if type_data == table_name:
		time_td = sql_tables_dict.get(table_name)
	elif sec_name is not False:
		time_td = sql_tables_dict.get(f'{type_data}.{table_name}.{sec_name}')
	else:
		time_td = sql_tables_dict.get(f'{type_data}.{table_name}')
	if deep is False:
		time_data = {table_name:campaigns_dict.get(table_name)}
		if time_data.get(table_name) is None:
			time_data = sql_tables_dict.get(f'{type_data}.{table_name}').copy()
			if 'special_data' in list(time_data.keys()):
				time_data.pop('special_data')
			for key in time_data.keys():
				time_data[key] = None
			time_data = json_normalize(time_data)
			dict_dataframe = add_data_df(server_time, time_td, time_data, campaigns_dict, table_name, type_data,
										 sec_name, dict_dataframe)
			return dict_dataframe
		elif isinstance(campaigns_dict.get(table_name), list):
			dict_dataframe = add_data_df(server_time, time_td, json_normalize(time_data.get(table_name)),
										 campaigns_dict, table_name, type_data, sec_name, dict_dataframe)
			return dict_dataframe
	else:
		if sec_name is False and have_deep_key is False:
			time_data = dict(campaigns_dict.get(table_name).items())
		elif sec_name is False and isinstance(have_deep_key, dict):
			time_data = have_deep_key
		else:
			time_td = sql_tables_dict.get(f'{type_data}.{table_name}.{sec_name}').copy()
			for col_name in time_td.keys():
				if campaigns_dict.get(table_name).get(sec_name) is not None:
					if isinstance(campaigns_dict.get(table_name).get(sec_name), list):
						t_df = json_normalize(campaigns_dict.get(table_name).get(sec_name))
						dict_dataframe = add_data_df(server_time, time_td, t_df, campaigns_dict, table_name, type_data,
													 sec_name, dict_dataframe)
						return dict_dataframe
					elif isinstance(campaigns_dict.get(table_name).get(sec_name).get(col_name), list):
						t_df = pd.DataFrame(
							campaigns_dict.get(table_name).get(sec_name).get(col_name), columns=[col_name])
						dict_dataframe = add_data_df(server_time, time_td, t_df, campaigns_dict, table_name, type_data,
													 sec_name, dict_dataframe)
						return dict_dataframe
					elif isinstance(campaigns_dict.get(table_name).get(sec_name), dict):
						time_td[col_name] = campaigns_dict.get(
							campaigns_dict.get(table_name).get(sec_name).get(col_name))
				else:
					time_td[col_name] = None
			else:
				if 'special_data' in list(time_td.keys()):
					time_td.pop('special_data')
				time_data = dict(time_td.items())
				time_td = sql_tables_dict.get(f'{type_data}.{table_name}.{sec_name}')

	dict_dataframe = add_data_df(server_time, time_td, json_normalize(time_data), campaigns_dict, table_name, type_data,
								 sec_name, dict_dataframe)
	return dict_dataframe


def files_iterable(set_filenames, path, type_file, tables_sql, date_time):
	pathlib.Path(path).mkdir(parents=True, exist_ok=True)
	finish_dict_df = dict()

	if isinstance(set_filenames, set):
		have_data = False
		for filename in set_filenames:
			with open(f'{path}/{filename}', 'r') as f:
				file = json.load(f)
				if len(file) > 0:
					dict_df = separate_data(type_file, tables_sql, file, date_time)
				else:
					continue
			if have_data is False:
				have_data = True
				finish_dict_df = dict_df.copy()
			else:
				for key, items in finish_dict_df.items():
					finish_dict_df[key] = pd.concat([dict_df.get(key), items], ignore_index=True)

	elif isinstance(set_filenames, str):
		with open(f'{path}/{set_filenames}', 'r') as f:
			file = json.load(f)
			finish_dict_df = separate_data(type_file, tables_sql, file, date_time)

	return finish_dict_df


def separate_data(type_data, sql_tb, all_cam, server_time):
	dict_frame = make_dict_df(sql_tb)
	tables_into_bd = list(sql_tb.keys())
	for camp_dict in all_cam.get(type_data):
		finish_camp_dict = camp_dict.copy()
		for k in camp_dict.keys():
			additional_key = f'{type_data}.{k}'
			if isinstance(camp_dict.get(k), dict):
				upper_dict = camp_dict.get(k).copy()
				have_deep_key = False
				for deep_key in camp_dict.get(k).keys():
					if f'{type_data}.{k}.{deep_key}' in tables_into_bd:
						have_deep_key = True
						upper_dict.pop(deep_key)
						dict_frame = add_df(server_time, type_data, k, camp_dict, sql_tb, dict_frame, True, deep_key)
				else:
					finish_camp_dict.pop(k)
					if len(upper_dict) > 0 and have_deep_key is False:
						dict_frame = add_df(server_time, type_data, k, camp_dict, sql_tb, dict_frame, True)
					elif len(upper_dict) > 0 and have_deep_key is True:
						dict_frame = add_df(server_time, type_data, k, camp_dict, sql_tb, dict_frame, True, False,
											upper_dict)
			elif additional_key in tables_into_bd:
				finish_camp_dict.pop(k)
				dict_frame = add_df(server_time, type_data, k, camp_dict, sql_tb, dict_frame)
		else:
			old_df = dict_frame.get(type_data)
			add_data = sql_tb.get(type_data).get('special_data').get('add_data')
			if add_data is not None and 'ServerDateTime' in add_data.keys():
				finish_camp_dict['ServerDateTime'] = server_time
			fin_df = pd.concat([old_df, json_normalize(finish_camp_dict)], ignore_index=True)
			dict_frame[type_data] = fin_df
	return dict_frame


def get_data_from_yandex(text_query, extention_url, request_metod, filename, file_name_mask, server_date_time=None):
	url, headers, p = get_conf()
	pathlib.Path(p).mkdir(parents=True, exist_ok=True)
	url = url + extention_url
	filename_set = set()

	if isinstance(filename, str):
		with open(f'{p}/{filename}', 'rb') as fh:
			data_file = pickle.load(fh)
	else:
		data_file = filename

	for number in data_file:
		payload = json.dumps(eval(text_query))
		try:
			response = requests.request(request_metod, url, headers=headers, data=payload)
		except AirflowWebServerTimeout as err:
			print(f'AirflowWebServerTimeout\n{err}')
		else:
			get_logs = json.loads(response.text)
			if response.status_code == 200:
				get_logs = request_error(get_logs)
				print(f'Старт записи {number}')

				filename = f'{file_name_mask}{number}.json'
				with open(f'{p}/{filename}', 'w') as f:
					json.dump(get_logs['result'], f)

				filename_set.add(filename)
				print('Финиш записи')

	return filename_set


def del_cycle(del_list):
	url, headers, p = get_conf()

	for elem in del_list:
		if elem is None:
			continue
		elif isinstance(elem, set):
			for set_elem in elem:
				if os.path.isfile(f'{p}/{set_elem}'):
					try:
						os.remove(f'{p}/{set_elem}')
					except Exception as err:
						print(err)
						raise AirflowFailException(f"Can't delete pickle data {set_elem}")
		elif isinstance(elem, str):
			if os.path.isfile(f'{p}/{elem}'):
				try:
					os.remove(f'{p}/{elem}')
				except Exception as err:
					print(err)
					raise AirflowFailException(f"Can't delete pickle data {elem}")


def _get_server_time(**kwargs):
	url, headers, p = get_conf()
	url = url + 'changes'
	payload = json.dumps({
		"method":"checkDictionaries",
		"params":{}
	})
	try:
		response = requests.request("POST", url, headers=headers, data=payload)
	except AirflowWebServerTimeout as err:
		print('AirflowWebServerTimeout', err)
	else:
		get_logs = json.loads(response.text)
		if response.status_code == 200:
			get_logs = request_error(get_logs)
			server_time = pendulum.parse(get_logs['result'].get('Timestamp'), strict=True)
			kwargs['task_instance'].xcom_push(
				key='server_data',
				value={
					'server_date_time':server_time.to_iso8601_string(),
					'mask_file_name':response.headers.get("RequestId")
				}
			)


def _first_branch(**kwargs):
	if kwargs['start_date'] == pendulum.today("UTC"):
		return ['full_load.create_table']
	else:
		return ['ch_campaigns.python_sensor']


def _create_table():
	connection, cursor = connect_db()
	request = 'CREATE TABLE IF NOT EXISTS "ServerTime" (dt timestamp)'
	cursor.execute(request)
	connection.commit()
	cursor.close()
	connection.close()
	connection, cursor = connect_db()

	def make_tab(d_data, cur, con):
		for key in d_data.keys():
			time_dict = d_data.get(key).copy()
			special_command = None
			if 'special_data' in time_dict.keys():
				special_command = change_query(time_dict.get('special_data'))
				time_dict.pop('special_data')
			df = json_normalize(time_dict)
			time_list = []
			for num in df:
				time_list.append(f'"{num}" {df[num].values[0]}')
			time_str = ','.join(time_list)
			if isinstance(special_command, str):
				time_str = f'{time_str}{special_command}'
			query = f'CREATE TABLE IF NOT EXISTS "{key}" ({time_str})'
			print(query)
			cur.execute(query)
			con.commit()
		return cur, con

	dict_data = dict(Variable.get("dictionaries", deserialize_json=True))
	cursor, connection = make_tab(dict_data, cursor, connection)
	dict_data = dict(Variable.get("SQL_Tables_Schema", deserialize_json=True))
	cursor, connection = make_tab(dict_data, cursor, connection)

	cursor.close()
	connection.close()


def _get_ad_extension(**kwargs):
	date_xcom = kwargs['ti'].xcom_pull(task_ids='get_server_time', key='server_data')
	mask_file_name = date_xcom.get("mask_file_name")

	url, headers, p = get_conf()
	url = url + 'adextensions'
	payload = json.dumps({
		"method":"get",
		"params":{
			"SelectionCriteria":{},
			"FieldNames":[
				"Id",
				"Associated",
				"Type",
				"State",
				"Status",
				"StatusClarification"
			],
			"CalloutFieldNames":[
				"CalloutText"
			]
		}
	})
	try:
		response = requests.request("POST", url, headers=headers, data=payload)
	except AirflowWebServerTimeout as err:
		print(f'AirflowWebServerTimeout\n{err}')
	else:
		get_logs = json.loads(response.text)
		if response.status_code == 200:
			get_logs = request_error(get_logs)
			print("Старт записи AdExtension")
			pathlib.Path(p).mkdir(parents=True, exist_ok=True)
			filename = f'AdExtensions_{mask_file_name}.json'
			with open(f'{p}/{filename}', 'w') as f:
				json.dump(get_logs['result'], f)
			kwargs['task_instance'].xcom_push(
				key='AdExtensions',
				value={
					'file_name':filename
				}
			)
			print("Финиш записи AdExtension")


def _separate_adextension_data(**kwargs):
	url, headers, p = get_conf()
	sql_tables = dict(Variable.get("SQL_Tables_Schema", deserialize_json=True))
	date_xcom = kwargs['ti'].xcom_pull(task_ids='get_server_time', key="server_data")
	server_date_time = date_xcom.get('server_date_time')
	mask_file_name = date_xcom.get("mask_file_name")
	date_xcom = kwargs['ti'].xcom_pull(task_ids=f'full_load.ad_extension.get_AdExtension', key="AdExtensions")
	filename = date_xcom.get('file_name')

	f_dict_df = files_iterable(filename, p, 'AdExtensions', sql_tables, server_date_time)

	filename = f'dict_AdExtensions_{mask_file_name}.pkl'
	with open(f'{p}/{filename}', 'wb') as f:
		pickle.dump(f_dict_df, f)

	kwargs['task_instance'].xcom_push(
		key='AdExtension',
		value={
			'file_name':filename
		}
	)


def _add_adextension_to_postgres(**kwargs):
	url, headers, p = get_conf()
	date_xcom = kwargs['ti'].xcom_pull(task_ids='full_load.ad_extension.separate_AdExtension_data', key='AdExtension')
	with open(f'{p}/{date_xcom.get("file_name")}', 'rb') as f:
		dict_df = pickle.load(f)

	write_data_to_postgres(dict_df)


def _get_site_links_set(**kwargs):
	date_xcom = kwargs['ti'].xcom_pull(task_ids='get_server_time', key='server_data')
	mask_file_name = date_xcom.get("mask_file_name")

	url, headers, p = get_conf()
	url = url + 'sitelinks'
	payload = json.dumps({
		"method":"get",
		"params":{
			"FieldNames":[
				"Id"
			],
			"SitelinkFieldNames":[
				"Title",
				"Href",
				"Description",
				"TurboPageId"
			]
		}
	})
	try:
		response = requests.request("POST", url, headers=headers, data=payload)
	except AirflowWebServerTimeout as err:
		print(f'AirflowWebServerTimeout\n{err}')
	else:
		get_logs = json.loads(response.text)
		if response.status_code == 200:
			get_logs = request_error(get_logs)
			print("Старт записи Site Links Set")
			pathlib.Path(p).mkdir(parents=True, exist_ok=True)
			filename = f'SiteLinksSet_{mask_file_name}.json'
			with open(f'{p}/{filename}', 'w') as f:
				json.dump(get_logs['result'], f)
			kwargs['task_instance'].xcom_push(
				key='SiteLinksSet',
				value={
					'file_name':filename
				}
			)
			print("Финиш записи Site Links Set")


def _separate_site_links_set_data(**kwargs):
	url, headers, p = get_conf()
	sql_tables = dict(Variable.get("SQL_Tables_Schema", deserialize_json=True))
	date_xcom = kwargs['ti'].xcom_pull(task_ids='get_server_time', key="server_data")
	server_date_time = date_xcom.get('server_date_time')
	mask_file_name = date_xcom.get("mask_file_name")
	date_xcom = kwargs['ti'].xcom_pull(task_ids=f'full_load.site_links.get_SiteLinksSet', key="SiteLinksSet")
	filename = date_xcom.get('file_name')

	f_dict_df = files_iterable(filename, p, 'SitelinksSets', sql_tables, server_date_time)

	filename = f'dict_SitelinksSets_{mask_file_name}.pkl'
	with open(f'{p}/{filename}', 'wb') as f:
		pickle.dump(f_dict_df, f)

	kwargs['task_instance'].xcom_push(
		key='SiteLinksSet',
		value={
			'file_name':filename
		}
	)


def _add_site_links_set_to_postgres(**kwargs):
	url, headers, p = get_conf()
	date_xcom = kwargs['ti'].xcom_pull(task_ids='full_load.site_links.separate_SiteLinksSet', key='SiteLinksSet')
	with open(f'{p}/{date_xcom.get("file_name")}', 'rb') as f:
		dict_df = pickle.load(f)

	write_data_to_postgres(dict_df)


def _get_dictionaries(**kwargs):
	date_xcom = kwargs['ti'].xcom_pull(task_ids='get_server_time', key='server_data')
	mask_file_name = date_xcom.get("mask_file_name")

	url, headers, p = get_conf()
	url = url + 'dictionaries'
	payload = json.dumps({
		"method":"get",
		"params":{
			"DictionaryNames":[
				"GeoRegions",
				"AdCategories",
				"Constants",
				"TimeZones",
				"SupplySidePlatforms",
				"AudienceCriteriaTypes",
				"AudienceDemographicProfiles",
				"AudienceInterests",
				"Interests"
			]
		}
	})
	try:
		response = requests.request("POST", url, headers=headers, data=payload)
	except AirflowWebServerTimeout as err:
		print('AirflowWebServerTimeout', err)
	else:
		get_logs = json.loads(response.text)
		if response.status_code == 200:
			get_logs = request_error(get_logs)
			pathlib.Path(p).mkdir(parents=True, exist_ok=True)
			filename = f'dictionaries_{mask_file_name}.json'
			with open(f'{p}/{filename}', 'w') as f:
				json.dump(get_logs['result'], f)

			kwargs['task_instance'].xcom_push(
				key='dictionaries',
				value={
					'file_name':filename
				}
			)


def _separate_dictionaries(**kwargs):
	url, headers, p = get_conf()
	sql_tables = dict(Variable.get("dictionaries", deserialize_json=True))
	date_xcom = kwargs['ti'].xcom_pull(task_ids='get_server_time', key="server_data")
	server_date_time = date_xcom.get('server_date_time')
	mask_file_name = date_xcom.get("mask_file_name")
	date_xcom = kwargs['ti'].xcom_pull(task_ids=f'full_load.download_dict.get_dictionaries', key="dictionaries")
	dict_filename = date_xcom.get('file_name')

	filename_set = set()
	for type_dict in sql_tables.keys():
		f_dict_df = files_iterable(dict_filename, p, type_dict, sql_tables, server_date_time)

		filename = f'{type_dict}_{mask_file_name}.pkl'
		with open(f'{p}/{filename}', 'wb') as f:
			pickle.dump(f_dict_df, f)
			filename_set.add(filename)

	kwargs['task_instance'].xcom_push(
		key='sep_dictionaries',
		value={
			'file_name':filename_set
		}
	)


def _add_dictionaries_to_bd(**kwargs):
	url, headers, p = get_conf()
	date_xcom = kwargs['ti'].xcom_pull(task_ids=f'full_load.download_dict.separate_dictionaries',
									   key="sep_dictionaries")
	filename_set = date_xcom.get("file_name")
	for file_name in filename_set:
		with open(f'{p}/{file_name}', 'rb') as f:
			dict_df = pickle.load(f)

		write_data_to_postgres(dict_df)


def _get_campaigns(**kwargs):
	date_xcom = kwargs['ti'].xcom_pull(task_ids='get_server_time', key='server_data')
	mask_file_name = date_xcom.get("mask_file_name")

	url, headers, p = get_conf()
	url = url + 'campaigns'
	payload = json.dumps({
		"method":"get",
		"params":{
			"SelectionCriteria":{},
			"FieldNames":[
				"Id",
				"Name",
				"ClientInfo",
				"StartDate",
				"EndDate",
				"TimeTargeting",
				"TimeZone",
				"NegativeKeywords",
				"BlockedIps",
				"ExcludedSites",
				"DailyBudget",
				"Notification",
				"Type",
				"Status",
				"State",
				"StatusPayment",
				"StatusClarification",
				"SourceId",
				"Statistics",
				"Currency",
				"Funds",
				"RepresentedBy"
			]
			, "TextCampaignFieldNames":[
				"Settings",
				"CounterIds",
				"RelevantKeywords",
				"BiddingStrategy",
				"PriorityGoals",
				"AttributionModel"
			]
		}
	})
	try:
		response = requests.request("GET", url, headers=headers, data=payload)
	except AirflowWebServerTimeout as err:
		print(f'AirflowWebServerTimeout\n{err}')
	else:
		get_logs = json.loads(response.text)
		if response.status_code == 200:
			get_logs = request_error(get_logs)
			print('Старт записи campaigns')
			pathlib.Path(p).mkdir(parents=True, exist_ok=True)
			filename = f'campaigns_{mask_file_name}.json'
			with open(f'{p}/{filename}', 'w') as f:
				json.dump(get_logs['result'], f)
			kwargs['task_instance'].xcom_push(
				key='campaigns',
				value={
					'file_name':filename
				}
			)
			print('Финиш записи campaigns')


def _separate_campaigns_data(**kwargs):
	url, headers, p = get_conf()
	sql_tables = dict(Variable.get("SQL_Tables_Schema", deserialize_json=True))
	date_xcom = kwargs['ti'].xcom_pull(task_ids='get_server_time', key='server_data')
	server_date_time = date_xcom.get('server_date_time')
	mask_file_name = date_xcom.get("mask_file_name")
	date_xcom = kwargs['ti'].xcom_pull(task_ids='full_load.campaign.get_campaigns', key='campaigns')
	filename = date_xcom.get("file_name")

	f_dict_df = files_iterable(filename, p, 'Campaigns', sql_tables, server_date_time)

	pathlib.Path(p).mkdir(parents=True, exist_ok=True)
	filename = f'All_companies_{mask_file_name}.pkl'

	with open(f'{p}/{filename}', 'wb') as f:
		pickle.dump(f_dict_df, f)

	kwargs['task_instance'].xcom_push(
		key='companies_df',
		value={
			'companies_df':filename,
			'companies_id':set(f_dict_df['Campaigns']['Id'])
		}
	)


def _add_companies_to_postgres(**kwargs):
	url, headers, p = get_conf()
	date_xcom = kwargs['ti'].xcom_pull(task_ids='full_load.campaign.separate_campaigns_data', key='companies_df')
	with open(f'{p}/{date_xcom.get("companies_df")}', 'rb') as f:
		dict_df = pickle.load(f)

	write_data_to_postgres(dict_df)


def _get_adgroup(**kwargs):
	date_xcom = kwargs['ti'].xcom_pull(task_ids='full_load.campaign.separate_campaigns_data', key="companies_df")
	filename = date_xcom['companies_id']

	date_xcom = kwargs['ti'].xcom_pull(task_ids='get_server_time', key='server_data')
	mask_file_name = date_xcom.get("mask_file_name")

	payload = '''{
            "method": "get",
            "params": {
                "SelectionCriteria": {
                    "CampaignIds": [
                        number
                    ]
                },
                "FieldNames": [
                    "Id",
                    "Name",
                    "CampaignId",
                    "RegionIds",
                    "NegativeKeywords",
                    "NegativeKeywordSharedSetIds",
                    "TrackingParams",
                    "RestrictedRegionIds",
                    "Status",
                    "ServingStatus",
                    "Type",
                    "Subtype"
                ],
                "TextAdGroupFeedParamsFieldNames": [
                    "FeedId",
                    "FeedCategoryIds"
                ],
                "MobileAppAdGroupFieldNames": [
                    "StoreUrl",
                    "TargetDeviceType",
                    "TargetCarrier",
                    "TargetOperatingSystemVersion",
                    "AppIconModeration",
                    "AppOperatingSystemType",
                    "AppAvailabilityStatus"
                ],
                "DynamicTextAdGroupFieldNames": [
                    "DomainUrl",
                    "DomainUrlProcessingStatus",
                    "AutotargetingCategories"
                ],
                "DynamicTextFeedAdGroupFieldNames": [
                    "Source",
                    "FeedId",
                    "SourceType",
                    "SourceProcessingStatus",
                    "AutotargetingCategories"
                ],
                "SmartAdGroupFieldNames": [
                    "FeedId",
                    "AdTitleSource",
                    "AdBodySource"
                ]
            }
        }'''
	extention_url = 'adgroups'
	req_metod = "POST"
	file_name_mask = f'AdGroups_Camp_number_{mask_file_name}_'

	filename_set = get_data_from_yandex(payload, extention_url, req_metod, filename, file_name_mask)

	kwargs['task_instance'].xcom_push(
		key='adgroups',
		value={
			'all_adgroups':filename_set
		}
	)


def _separate_adgroups_data(**kwargs):
	url, headers, p = get_conf()
	sql_tables = dict(Variable.get("SQL_Tables_Schema", deserialize_json=True))
	date_xcom = kwargs['ti'].xcom_pull(task_ids=f'full_load.adgroup.get_adgroup', key="adgroups")
	filename_set = date_xcom.get('all_adgroups')
	date_xcom = kwargs['ti'].xcom_pull(task_ids='get_server_time', key="server_data")
	server_date_time = date_xcom.get('server_date_time')
	mask_file_name = date_xcom.get("mask_file_name")

	f_dict_df = files_iterable(filename_set, p, 'AdGroups', sql_tables, server_date_time)

	# Разделяем регионы
	time_plus = pd.DataFrame(columns=['Id', 'RegionIds', 'ServerDateTime'], data=None)
	time_minus = pd.DataFrame(columns=['Id', 'RegionIds', 'ServerDateTime'], data=None)
	time_r_r_ids = pd.DataFrame(columns=['Id', 'Items', 'ServerDateTime'], data=None)

	for row, series in f_dict_df['AdGroups'].iterrows():
		for reg_id in series['RegionIds']:
			data = [series['Id'], abs(reg_id), series['ServerDateTime']]
			if reg_id >= 0:
				time_plus = pd.concat(
					[time_plus, pd.DataFrame(columns=['Id', 'RegionIds', 'ServerDateTime'], data=[data])])
			else:
				time_minus = pd.concat(
					[time_plus, pd.DataFrame(columns=['Id', 'RegionIds', 'ServerDateTime'], data=[data])])
	else:
		del f_dict_df['AdGroups']['RegionIds']
		f_dict_df['AdGroups.RegionIds.Plus'] = time_plus
		f_dict_df['AdGroups.RegionIds.Minus'] = time_minus

	t_df = f_dict_df['AdGroups.RestrictedRegionIds'][f_dict_df['AdGroups.RestrictedRegionIds']['Items'].notna()]
	for row, series in t_df.iterrows():
		for reg_id in series['Items']:
			data = [series['Id'], reg_id, series['ServerDateTime']]
			time_r_r_ids = pd.concat(
				[time_r_r_ids, pd.DataFrame(columns=['Id', 'Items', 'ServerDateTime'], data=[data])])
	else:
		f_dict_df['AdGroups.RestrictedRegionIds'] = time_r_r_ids

	filename = f'dict_all_adgroups_{mask_file_name}.pkl'
	with open(f'{p}/{filename}', 'wb') as f:
		pickle.dump(f_dict_df, f)

	kwargs['task_instance'].xcom_push(
		key='all_adgroups',
		value={
			'file_name':filename,
			'adgroups_id':set(f_dict_df['AdGroups']['Id'])
		}
	)


def _add_adgroups_to_postgres(**kwargs):
	url, headers, p = get_conf()
	date_xcom = kwargs['ti'].xcom_pull(task_ids='full_load.adgroup.separate_adgroups_data', key='all_adgroups')
	with open(f'{p}/{date_xcom.get("file_name")}', 'rb') as f:
		dict_df = pickle.load(f)

	write_data_to_postgres(dict_df)


def _get_ad(**kwargs):
	date_xcom = kwargs['ti'].xcom_pull(task_ids='full_load.adgroup.separate_adgroups_data', key="all_adgroups")
	adgroups_id_set = date_xcom['adgroups_id']
	date_xcom = kwargs['ti'].xcom_pull(task_ids='get_server_time', key='server_data')
	mask_file_name = date_xcom.get("mask_file_name")

	payload = '''{
            "method": "get",
            "params": {
                "SelectionCriteria": {
                    "AdGroupIds": [
                        number
                    ]
                },
                "FieldNames": [
                    "AdCategories",
                    "AgeLabel",
                    "AdGroupId",
                    "Id",
                    "CampaignId",
                    "State",
                    "Status",
                    "StatusClarification",
                    "Type",
                    "Subtype"
                ],
                "TextAdFieldNames": [
                    "Title",
                    "Title2",
                    "Text",
                    "Href",
                    "Mobile",
                    "DisplayDomain",
                    "DisplayUrlPath",
                    "VCardId",
                    "AdImageHash",
                    "SitelinkSetId",
                    "DisplayUrlPathModeration",
                    "VCardModeration",
                    "SitelinksModeration",
                    "AdImageModeration",
                    "AdExtensions",
                    "VideoExtension",
                    "TurboPageId",
                    "TurboPageModeration",
                    "BusinessId",
                    "PreferVCardOverBusiness"
                ],
                "TextAdPriceExtensionFieldNames": [
                    "Price",
                    "OldPrice",
                    "PriceCurrency",
                    "PriceQualifier"
                ],
                "MobileAppAdFieldNames": [
                    "Title",
                    "Text",
                    "TrackingUrl",
                    "Action",
                    "AdImageHash",
                    "Features",
                    "AdImageModeration",
                    "VideoExtension"
                ],
                "DynamicTextAdFieldNames": [
                    "VCardId",
                    "AdImageHash",
                    "SitelinkSetId",
                    "VCardModeration",
                    "SitelinksModeration",
                    "AdImageModeration",
                    "AdExtensions",
                    "Text"
                ],
                "TextImageAdFieldNames": [
                    "AdImageHash",
                    "Href",
                    "TurboPageId",
                    "TurboPageModeration"
                ],
                "MobileAppImageAdFieldNames": [
                    "AdImageHash",
                    "TrackingUrl"
                ],
                "TextAdBuilderAdFieldNames": [
                    "Creative",
                    "Href",
                    "TurboPageId",
                    "TurboPageModeration"
                ],
                "MobileAppAdBuilderAdFieldNames": [
                    "Creative",
                    "TrackingUrl"
                ],
                "CpcVideoAdBuilderAdFieldNames": [
                    "Creative",
                    "Href",
                    "TurboPageId",
                    "TurboPageModeration"
                ],
                "MobileAppCpcVideoAdBuilderAdFieldNames": [
                    "Creative",
                    "TrackingUrl"
                ],
                "CpmBannerAdBuilderAdFieldNames": [
                    "Creative",
                    "Href",
                    "TrackingPixels",
                    "TurboPageId",
                    "TurboPageModeration"
                ],
                "CpmVideoAdBuilderAdFieldNames": [
                    "Creative",
                    "Href",
                    "TrackingPixels",
                    "TurboPageId",
                    "TurboPageModeration"
                ]
            }
        }'''
	extention_url = 'ads'
	req_metod = "POST"
	file_name_mask = f'Ad_Groups_number_{mask_file_name}_'

	filename_set = get_data_from_yandex(payload, extention_url, req_metod, adgroups_id_set, file_name_mask)

	kwargs['task_instance'].xcom_push(
		key='ads',
		value={
			'all_ads':filename_set
		}
	)


def _separate_ad_data(**kwargs):
	url, headers, p = get_conf()
	sql_tables = dict(Variable.get("SQL_Tables_Schema", deserialize_json=True))
	date_xcom = kwargs['ti'].xcom_pull(task_ids=f'full_load.ad.get_ad', key="ads")
	filename_set = date_xcom.get('all_ads')
	date_xcom = kwargs['ti'].xcom_pull(task_ids='get_server_time', key="server_data")
	server_date_time = date_xcom.get('server_date_time')
	mask_file_name = date_xcom.get("mask_file_name")

	f_dict_df = files_iterable(filename_set, p, 'Ads', sql_tables, server_date_time)

	filename = f'dict_all_ads_{mask_file_name}.pkl'
	with open(f'{p}/{filename}', 'wb') as f:
		pickle.dump(f_dict_df, f)

	kwargs['task_instance'].xcom_push(
		key='all_ads',
		value={
			'file_name':filename
		}
	)


def _add_ad_to_postgres(**kwargs):
	url, headers, p = get_conf()
	date_xcom = kwargs['ti'].xcom_pull(task_ids='full_load.ad.separate_ad_data', key='all_ads')
	with open(f'{p}/{date_xcom.get("file_name")}', 'rb') as f:
		dict_df = pickle.load(f)

	write_data_to_postgres(dict_df)


def _add_server_time_to_bd(**kwargs):
	connection, cursor = connect_db()
	date_xcom = kwargs['ti'].xcom_pull(task_ids=f'get_server_time', key="server_data")
	date_time = date_xcom.get('server_date_time')
	try:
		""" Into data to the Postgres """
		table_name = '"ServerTime"'
		sql_query = f"INSERT INTO public.{table_name} (dt) VALUES('{date_time}')"
		cursor.execute(sql_query)

		""" END into data to the Postgres """
	except Exception as err:
		print(err)
		raise AirflowFailException("Can't write to database")
	else:
		cursor.close()
		connection.commit()
		connection.close()


def _get_audience_targets(**kwargs):
	date_xcom = kwargs['ti'].xcom_pull(task_ids='get_server_time', key='server_data')
	mask_file_name = date_xcom.get("mask_file_name")

	date_xcom = kwargs['ti'].xcom_pull(task_ids='full_load.campaign.separate_campaigns_data', key="companies_df")
	filename = date_xcom['companies_id']

	payload = '''{
            "method": "get",
            "params": {
                "SelectionCriteria": {
                    "CampaignIds": [
                        number
                    ]
                },
                "FieldNames": [
                    "Id",
                    "AdGroupId",
                    "CampaignId",
                    "RetargetingListId",
                    "InterestId",
                    "ContextBid",
                    "StrategyPriority",
                    "State"
                ]
            }
        }'''
	extention_url = 'audiencetargets'
	req_metod = "POST"
	file_name_mask = f'AudienceTargets_Camp_number_{mask_file_name}_'

	filename_set = get_data_from_yandex(payload, extention_url, req_metod, filename, file_name_mask)

	kwargs['task_instance'].xcom_push(
		key='AudienceTargets',
		value={
			'file_name':filename_set
		}
	)


def _separate_audience_data(**kwargs):
	url, headers, p = get_conf()
	sql_tables = dict(Variable.get("SQL_Tables_Schema", deserialize_json=True))
	date_xcom = kwargs['ti'].xcom_pull(task_ids=f'full_load.audience_targets.get_audience_targets',
									   key="AudienceTargets")
	filename_set = date_xcom.get('file_name')
	date_xcom = kwargs['ti'].xcom_pull(task_ids='get_server_time', key="server_data")
	server_date_time = date_xcom.get('server_date_time')
	mask_file_name = date_xcom.get("mask_file_name")

	f_dict_df = files_iterable(filename_set, p, 'AudienceTargets', sql_tables, server_date_time)

	filename = f'dict_all_audience_targets_{mask_file_name}.pkl'
	with open(f'{p}/{filename}', 'wb') as f:
		pickle.dump(f_dict_df, f)

	kwargs['task_instance'].xcom_push(
		key='all_audience_targets',
		value={
			'file_name':filename
		}
	)


def _add_audience_data_to_postgres(**kwargs):
	url, headers, p = get_conf()
	date_xcom = kwargs['ti'].xcom_pull(task_ids='full_load.audience_targets.separate_audience_data',
									   key='all_audience_targets')
	with open(f'{p}/{date_xcom.get("file_name")}', 'rb') as f:
		dict_df = pickle.load(f)

	write_data_to_postgres(dict_df)


def _get_keywords(**kwargs):
	date_xcom = kwargs['ti'].xcom_pull(task_ids='get_server_time', key='server_data')
	mask_file_name = date_xcom.get("mask_file_name")
	date_xcom = kwargs['ti'].xcom_pull(task_ids='full_load.campaign.separate_campaigns_data', key="companies_df")
	filename = date_xcom['companies_id']

	payload = '''{
            "method": "get",
            "params": {
                "SelectionCriteria": {
                    "CampaignIds": [
                        number
                    ]
                },
                "FieldNames": [
                    "Id",
                    "Keyword",
                    "State",
                    "Status",
                    "ServingStatus",
                    "AdGroupId",
                    "CampaignId",
                    "Bid",
                    "ContextBid",
                    "StrategyPriority",
                    "UserParam1",
                    "UserParam2",
                    "StatisticsSearch",
                    "StatisticsNetwork",
                    "AutotargetingCategories"
                ]
            }
        }'''
	extention_url = 'keywords'
	req_metod = "POST"
	file_name_mask = f'Keyword_Camp_number_{mask_file_name}_'

	filename_set = get_data_from_yandex(payload, extention_url, req_metod, filename, file_name_mask)

	kwargs['task_instance'].xcom_push(
		key='keyword',
		value={
			'file_name':filename_set
		}
	)


def _separate_keywords_data(**kwargs):
	url, headers, p = get_conf()
	sql_tables = dict(Variable.get("SQL_Tables_Schema", deserialize_json=True))
	date_xcom = kwargs['ti'].xcom_pull(task_ids=f'full_load.keyword.get_keywords', key="keyword")
	filename_set = date_xcom.get('file_name')
	date_xcom = kwargs['ti'].xcom_pull(task_ids='get_server_time', key="server_data")
	server_date_time = date_xcom.get('server_date_time')
	mask_file_name = date_xcom.get("mask_file_name")

	f_dict_df = files_iterable(filename_set, p, 'Keywords', sql_tables, server_date_time)

	filename = f'dict_all_keyword_{mask_file_name}.pkl'
	with open(f'{p}/{filename}', 'wb') as f:
		pickle.dump(f_dict_df, f)

	kwargs['task_instance'].xcom_push(
		key='all_keyword',
		value={
			'file_name':filename
		}
	)


def _add_keywords_to_postgres(**kwargs):
	url, headers, p = get_conf()
	date_xcom = kwargs['ti'].xcom_pull(task_ids='full_load.keyword.separate_keywords_data', key='all_keyword')
	with open(f'{p}/{date_xcom.get("file_name")}', 'rb') as f:
		dict_df = pickle.load(f)

	write_data_to_postgres(dict_df)


def _python_sensor(**kwargs):
	# на отладку

	date_xcom = kwargs['ti'].xcom_pull(task_ids='get_server_time', key='server_data')
	server_date_time = pendulum.parse(date_xcom.get("server_date_time"))

	rez = get_load_date(**kwargs)

	if server_date_time.add(days=-1).date() != pendulum.instance(rez[0][0]).date():
		print('First start')
		pend_time = pendulum.instance(rez[0][0])
		kwargs['task_instance'].xcom_push(
			key='previous_load_date',
			value={
				'server_date_time':pend_time.to_iso8601_string()
			}
		)
		return True
	else:
		return False


def _check_campaigns(**kwargs):
	date_xcom = kwargs['ti'].xcom_pull(task_ids=f'ch_campaigns.python_sensor', key="previous_load_date")
	server_date_time = date_xcom['server_date_time']
	url, headers, p = get_conf()
	pathlib.Path(p).mkdir(parents=True, exist_ok=True)
	url = url + 'changes'
	payload = json.dumps({
		"method":"checkCampaigns",
		"params":{
			"Timestamp":f"{server_date_time}"
		}
	})
	try:
		response = requests.request("POST", url, headers=headers, data=payload)
	except AirflowWebServerTimeout as err:
		print('AirflowWebServerTimeout', err)
	else:
		get_logs = json.loads(response.text)
		if response.status_code == 200:
			get_logs = request_error(get_logs)
			filename = f'checkCampaigns_{response.headers.get("RequestId")}.json'
			print('Старт записи checkCampaigns')
			with open(f'{p}/{filename}', 'w') as f:
				json.dump(get_logs['result'], f)

			kwargs['task_instance'].xcom_push(
				key='checkCampaigns',
				value={
					'file_name':filename,
					'server_date_time':get_logs.get('result').get('Timestamp'),
					'mask_file_name':response.headers.get("RequestId")
				}
			)


def _pre_processing_check_campaigns(**kwargs):
	url, headers, p = get_conf()
	pathlib.Path(p).mkdir(parents=True, exist_ok=True)
	date_xcom = kwargs['ti'].xcom_pull(task_ids=f'ch_campaigns.update_data.check_campaigns', key="checkCampaigns")
	filename = date_xcom.get('file_name')
	mask_file_name = date_xcom.get("mask_file_name")

	with open(f'{p}/{filename}', 'r', encoding='utf-8') as fh:
		data_file = json.load(fh)

	self_set = set()
	children_set = set()
	stat_set = set()

	if data_file.get('Campaigns') is None:
		raise AirflowSkipException("Нет обновленния checkCampaigns")

	for elem in data_file.get('Campaigns'):
		campaign_id = elem.get('CampaignId')
		for type_change in elem.get('ChangesIn'):
			if 'SELF' in type_change:
				self_set.add(campaign_id)
			if 'CHILDREN' in type_change:
				children_set.add(campaign_id)
			if 'STAT' in type_change:
				stat_set.add(campaign_id)

	if len(self_set) > 0:
		with open(f'{p}/self_set_{mask_file_name}.pkl', 'wb') as f:
			pickle.dump(self_set, f)

	if len(children_set) > 0:
		with open(f'{p}/children_set_{mask_file_name}.pkl', 'wb') as f:
			pickle.dump(children_set, f)

	if len(stat_set) > 0:
		with open(f'{p}/stat_set_{mask_file_name}.pkl', 'wb') as f:
			pickle.dump(stat_set, f)

	kwargs['task_instance'].xcom_push(
		key='Check',
		value={
			'SELF': f'self_set_{mask_file_name}.pkl' if len(self_set) > 0 else None,
			'CHILDREN': f'children_set_{mask_file_name}.pkl' if len(children_set) > 0 else None,
			'STAT': f'stat_set_{mask_file_name}.pkl' if len(stat_set) > 0 else None
		}
	)


def _get_changed_children(**kwargs):
	date_xcom = kwargs['ti'].xcom_pull(task_ids=f'ch_campaigns.python_sensor', key="previous_load_date")
	server_date_time = date_xcom['server_date_time']
	date_xcom = kwargs['ti'].xcom_pull(task_ids=f'ch_campaigns.update_data.check_campaigns', key="checkCampaigns")
	mask_file_name = date_xcom.get("mask_file_name")
	date_xcom = kwargs['ti'].xcom_pull(task_ids=f'ch_campaigns.update_data.pre_processing_check_campaigns', key="Check")
	filename = date_xcom['CHILDREN']

	if filename is None:
		raise AirflowSkipException("Нет обновленния CHILDREN")

	payload = '''{
		"method": "check",
		"params": {
			"CampaignIds": [
			    number
			],
			"FieldNames": [
			    "AdGroupIds",
			    "AdIds"
			],
			"Timestamp": server_date_time
			}
		}'''
	extention_url = 'changes'
	req_metod = "GET"
	file_name_mask = f'children_campaigns_{mask_file_name}_'

	filename_set = get_data_from_yandex(payload, extention_url, req_metod, filename, file_name_mask, server_date_time)

	kwargs['task_instance'].xcom_push(
		key='campaigns',
		value={
			'file_name': filename_set
		}
	)


def _data_layout(**kwargs):
	url, headers, p = get_conf()
	pathlib.Path(p).mkdir(parents=True, exist_ok=True)
	date_xcom = kwargs['ti'].xcom_pull(task_ids=f'ch_campaigns.update_data.get_changed_children', key="campaigns")
	filename = date_xcom.get('file_name')
	date_xcom = kwargs['ti'].xcom_pull(task_ids=f'ch_campaigns.update_data.check_campaigns', key="checkCampaigns")
	mask_file_name = date_xcom.get("mask_file_name")

	ad_ids_set = set()
	ad_group_id_set = set()
	for f in filename:
		with open(f'{p}/{f}', 'r', encoding='utf-8') as fh:
			data_file = json.load(fh)
		for type_data in data_file.get('Modified').keys():
			for ids in data_file.get('Modified').get(type_data):
				if type_data == 'AdIds':
					ad_ids_set.add(ids)
				if type_data == 'AdGroupIds':
					ad_group_id_set.add(ids)

	if len(ad_ids_set) > 0:
		with open(f'{p}/ad_ids_set_{mask_file_name}.pkl', 'wb') as f:
			pickle.dump(ad_ids_set, f)

	with open(f'{p}/ad_group_id_set_{mask_file_name}.pkl', 'wb') as f:
		pickle.dump(ad_group_id_set, f)

	kwargs['task_instance'].xcom_push(
		key='processing',
		value={
			'AdIds':f'ad_ids_set_{mask_file_name}.pkl' if len(ad_ids_set) > 0 else None,
			'AdGroupIds':f'ad_group_id_set_{mask_file_name}.pkl'
		}
	)


def _get_adgroup_update(**kwargs):
	date_xcom = kwargs['ti'].xcom_pull(task_ids=f'ch_campaigns.update_data.check_campaigns', key="checkCampaigns")
	mask_file_name = date_xcom.get("mask_file_name")
	date_xcom = kwargs['ti'].xcom_pull(task_ids='ch_campaigns.update_data.data_layout', key="processing")
	filename = date_xcom['AdGroupIds']

	if filename is None:
		raise AirflowSkipException("Нет обновленния adgroups")

	payload = '''{
            "method": "get",
            "params": {
                "SelectionCriteria": {
                    "Ids": [
                        number
                    ]
                },
                "FieldNames": [
                    "Id",
                    "Name",
                    "CampaignId",
                    "RegionIds",
                    "NegativeKeywords",
                    "NegativeKeywordSharedSetIds",
                    "TrackingParams",
                    "RestrictedRegionIds",
                    "Status",
                    "ServingStatus",
                    "Type",
                    "Subtype"
                ],
                "TextAdGroupFeedParamsFieldNames": [
                    "FeedId",
                    "FeedCategoryIds"
                ],
                "MobileAppAdGroupFieldNames": [
                    "StoreUrl",
                    "TargetDeviceType",
                    "TargetCarrier",
                    "TargetOperatingSystemVersion",
                    "AppIconModeration",
                    "AppOperatingSystemType",
                    "AppAvailabilityStatus"
                ],
                "DynamicTextAdGroupFieldNames": [
                    "DomainUrl",
                    "DomainUrlProcessingStatus",
                    "AutotargetingCategories"
                ],
                "DynamicTextFeedAdGroupFieldNames": [
                    "Source",
                    "FeedId",
                    "SourceType",
                    "SourceProcessingStatus",
                    "AutotargetingCategories"
                ],
                "SmartAdGroupFieldNames": [
                    "FeedId",
                    "AdTitleSource",
                    "AdBodySource"
                ]
            }
        }'''
	extention_url = 'adgroups'
	req_metod = "POST"
	file_name_mask = f'AdGroups_update_{mask_file_name}_'

	filename_set = get_data_from_yandex(payload, extention_url, req_metod, filename, file_name_mask)

	kwargs['task_instance'].xcom_push(
		key='adgroups',
		value={
			'file_name':filename_set
		}
	)


def _separate_adgroups_update_data(**kwargs):
	url, headers, p = get_conf()
	date_xcom = kwargs['ti'].xcom_pull(task_ids=f'ch_campaigns.update_data.check_campaigns', key="checkCampaigns")
	mask_file_name = date_xcom.get("mask_file_name")
	server_date_time = date_xcom.get('server_date_time')
	sql_tables = dict(Variable.get("SQL_Tables_Schema", deserialize_json=True))
	date_xcom = kwargs['ti'].xcom_pull(
		task_ids=f'ch_campaigns.update_data.adgroup_update.get_adgroup_update',
		key="adgroups")
	filename_set = date_xcom.get('file_name')

	f_dict_df = files_iterable(filename_set, p, 'AdGroups', sql_tables, server_date_time)

	# Разделяем регионы
	time_plus = pd.DataFrame(columns=['Id', 'RegionIds', 'ServerDateTime'], data=None)
	time_minus = pd.DataFrame(columns=['Id', 'RegionIds', 'ServerDateTime'], data=None)
	time_r_r_ids = pd.DataFrame(columns=['Id', 'Items', 'ServerDateTime'], data=None)

	for row, series in f_dict_df['AdGroups'].iterrows():
		for reg_id in series['RegionIds']:
			data = [series['Id'], abs(reg_id), series['ServerDateTime']]
			if reg_id >= 0:
				time_plus = pd.concat(
					[time_plus, pd.DataFrame(columns=['Id', 'RegionIds', 'ServerDateTime'], data=[data])])
			else:
				time_minus = pd.concat(
					[time_plus, pd.DataFrame(columns=['Id', 'RegionIds', 'ServerDateTime'], data=[data])])
	else:
		del f_dict_df['AdGroups']['RegionIds']
		f_dict_df['AdGroups.RegionIds.Plus'] = time_plus
		f_dict_df['AdGroups.RegionIds.Minus'] = time_minus

	t_df = f_dict_df['AdGroups.RestrictedRegionIds'][f_dict_df['AdGroups.RestrictedRegionIds']['Items'].notna()]
	for row, series in t_df.iterrows():
		for reg_id in series['Items']:
			data = [series['Id'], reg_id, series['ServerDateTime']]
			time_r_r_ids = pd.concat(
				[time_r_r_ids, pd.DataFrame(columns=['Id', 'Items', 'ServerDateTime'], data=[data])])
	else:
		f_dict_df['AdGroups.RestrictedRegionIds'] = time_r_r_ids

	filename = f'dict_update_adgroups_{mask_file_name}.pkl'
	with open(f'{p}/{filename}', 'wb') as f:
		pickle.dump(f_dict_df, f)

	kwargs['task_instance'].xcom_push(
		key='update_adgroups',
		value={
			'file_name': filename,
			'adgroups_id': set(f_dict_df['AdGroups']['Id'])
		}
	)


def _add_adgroups_update_to_postgres(**kwargs):
	url, headers, p = get_conf()
	date_xcom = kwargs['ti'].xcom_pull(
		task_ids='ch_campaigns.update_data.adgroup_update.separate_adgroups_update_data',
		key='update_adgroups')

	with open(f'{p}/{date_xcom.get("file_name")}', 'rb') as f:
		dict_df = pickle.load(f)

	write_data_to_postgres(dict_df)


def _get_ad_update(**kwargs):
	date_xcom = kwargs['ti'].xcom_pull(task_ids=f'ch_campaigns.update_data.check_campaigns', key="checkCampaigns")
	mask_file_name = date_xcom.get("mask_file_name")
	date_xcom = kwargs['ti'].xcom_pull(task_ids='ch_campaigns.update_data.data_layout', key="processing")
	filename = date_xcom['AdIds']

	if filename is None:
		raise AirflowSkipException("Нет обновленния AdIds")

	payload = '''{
            "method": "get",
            "params": {
                "SelectionCriteria": {
                    "Ids": [
                        number
                    ]
                },
                "FieldNames": [
                    "AdCategories",
                    "AgeLabel",
                    "AdGroupId",
                    "Id",
                    "CampaignId",
                    "State",
                    "Status",
                    "StatusClarification",
                    "Type",
                    "Subtype"
                ],
                "TextAdFieldNames": [
                    "Title",
                    "Title2",
                    "Text",
                    "Href",
                    "Mobile",
                    "DisplayDomain",
                    "DisplayUrlPath",
                    "VCardId",
                    "AdImageHash",
                    "SitelinkSetId",
                    "DisplayUrlPathModeration",
                    "VCardModeration",
                    "SitelinksModeration",
                    "AdImageModeration",
                    "AdExtensions",
                    "VideoExtension",
                    "TurboPageId",
                    "TurboPageModeration",
                    "BusinessId",
                    "PreferVCardOverBusiness"
                ],
                "TextAdPriceExtensionFieldNames": [
                    "Price",
                    "OldPrice",
                    "PriceCurrency",
                    "PriceQualifier"
                ],
                "MobileAppAdFieldNames": [
                    "Title",
                    "Text",
                    "TrackingUrl",
                    "Action",
                    "AdImageHash",
                    "Features",
                    "AdImageModeration",
                    "VideoExtension"
                ],
                "DynamicTextAdFieldNames": [
                    "VCardId",
                    "AdImageHash",
                    "SitelinkSetId",
                    "VCardModeration",
                    "SitelinksModeration",
                    "AdImageModeration",
                    "AdExtensions",
                    "Text"
                ],
                "TextImageAdFieldNames": [
                    "AdImageHash",
                    "Href",
                    "TurboPageId",
                    "TurboPageModeration"
                ],
                "MobileAppImageAdFieldNames": [
                    "AdImageHash",
                    "TrackingUrl"
                ],
                "TextAdBuilderAdFieldNames": [
                    "Creative",
                    "Href",
                    "TurboPageId",
                    "TurboPageModeration"
                ],
                "MobileAppAdBuilderAdFieldNames": [
                    "Creative",
                    "TrackingUrl"
                ],
                "CpcVideoAdBuilderAdFieldNames": [
                    "Creative",
                    "Href",
                    "TurboPageId",
                    "TurboPageModeration"
                ],
                "MobileAppCpcVideoAdBuilderAdFieldNames": [
                    "Creative",
                    "TrackingUrl"
                ],
                "CpmBannerAdBuilderAdFieldNames": [
                    "Creative",
                    "Href",
                    "TrackingPixels",
                    "TurboPageId",
                    "TurboPageModeration"
                ],
                "CpmVideoAdBuilderAdFieldNames": [
                    "Creative",
                    "Href",
                    "TrackingPixels",
                    "TurboPageId",
                    "TurboPageModeration"
                ]
            }
        }'''
	extention_url = 'ads'
	req_metod = "POST"
	file_name_mask = f'Ad_number_update_{mask_file_name}_'

	filename_set = get_data_from_yandex(payload, extention_url, req_metod, filename, file_name_mask)

	print('Финиш скачивания Ad')
	kwargs['task_instance'].xcom_push(
		key='ads',
		value={
			'file_name':filename_set
		}
	)


def _separate_ad_update_data(**kwargs):
	url, headers, p = get_conf()
	sql_tables = dict(Variable.get("SQL_Tables_Schema", deserialize_json=True))

	date_xcom = kwargs['ti'].xcom_pull(task_ids=f'ch_campaigns.update_data.ad_update.get_ad_update', key="ads")
	filename_set = date_xcom.get('file_name')

	date_xcom = kwargs['ti'].xcom_pull(task_ids=f'ch_campaigns.update_data.check_campaigns', key="checkCampaigns")
	mask_file_name = date_xcom.get("mask_file_name")
	server_date_time = date_xcom.get('server_date_time')

	f_dict_df = files_iterable(filename_set, p, 'Ads', sql_tables, server_date_time)

	filename = f'dict_update_ads_{mask_file_name}.pkl'
	with open(f'{p}/{filename}', 'wb') as f:
		pickle.dump(f_dict_df, f)

	kwargs['task_instance'].xcom_push(
		key='update_ads',
		value={
			'file_name': filename,
			'ads_id': set(f_dict_df['Ads']['Id'])
		}
	)


def _add_ad_update_to_postgres(**kwargs):
	url, headers, p = get_conf()
	date_xcom = kwargs['ti'].xcom_pull(
		task_ids='ch_campaigns.update_data.ad_update.separate_ad_update_data',
		key='update_ads')
	with open(f'{p}/{date_xcom.get("file_name")}', 'rb') as f:
		dict_df = pickle.load(f)

	write_data_to_postgres(dict_df)


def _get_changed_campaigns(**kwargs):
	date_xcom = kwargs['ti'].xcom_pull(task_ids=f'ch_campaigns.update_data.check_campaigns', key="checkCampaigns")
	mask_file_name = date_xcom.get("mask_file_name")
	date_xcom = kwargs['ti'].xcom_pull(
		task_ids=f'ch_campaigns.update_data.pre_processing_check_campaigns',
		key="Check")
	if date_xcom is None:
		raise AirflowSkipException("No company data")
	filename = date_xcom.get('SELF')

	payload = '''{
        "method": "get",
        "params": {
            "SelectionCriteria": {
                "Ids": [number]
            },
            "FieldNames": [
                "Id",
                "Name",
                "ClientInfo",
                "StartDate",
                "EndDate",
                "TimeTargeting",
                "TimeZone",
                "NegativeKeywords",
                "BlockedIps",
                "ExcludedSites",
                "DailyBudget",
                "Notification",
                "Type",
                "Status",
                "State",
                "StatusPayment",
                "StatusClarification",
                "SourceId",
                "Currency",
                "Funds",
                "RepresentedBy"
            ]
            , "TextCampaignFieldNames": [
                "Settings",
                "CounterIds",
                "RelevantKeywords",
                "BiddingStrategy",
                "PriorityGoals",
                "AttributionModel"
            ]
            #            ,"SmartCampaignFieldNames": [
            #                "CounterId",
            #                "Settings",
            #                "BiddingStrategy",
            #                "PriorityGoals",
            #                "AttributionModel"
            #            ]
            #            ,"DynamicTextCampaignFieldNames": [
            #                "CounterIds",
            #                "Settings",
            #                "PlacementTypes",
            #                "BiddingStrategy",
            #                "PriorityGoals",
            #                "AttributionModel"
            #            ]
            #            ,"MobileAppCampaignFieldNames": [
            #                "Settings",
            #                "BiddingStrategy"
            #            ]
            #            ,"CpmBannerCampaignFieldNames": [
            #                "CounterIds",
            #                "FrequencyCap",
            #                "VideoTarget",
            #                "Settings",
            #                "BiddingStrategy"
            #            ]
        }
    }'''
	extention_url = 'campaigns'
	req_metod = "GET"
	file_name_mask = f'campaigns_{mask_file_name}_'

	filename_set = get_data_from_yandex(payload, extention_url, req_metod, filename, file_name_mask)

	url, headers, p = get_conf()
	pathlib.Path(p).mkdir(parents=True, exist_ok=True)
	if isinstance(filename, str):
		with open(f'{p}/{filename}', 'rb') as fh:
			data_file = pickle.load(fh)

	kwargs['task_instance'].xcom_push(
		key='campaigns',
		value={
			'file_name': filename_set,
			'campaigns_id': data_file
		}
	)


def _separate_changed_campaigns(**kwargs):
	url, headers, p = get_conf()
	sql_tables = dict(Variable.get("SQL_Tables_Schema", deserialize_json=True))
	date_xcom = kwargs['ti'].xcom_pull(task_ids=f'ch_campaigns.update_data.check_campaigns', key="checkCampaigns")
	server_date_time = date_xcom.get('server_date_time')
	mask_file_name = date_xcom.get("mask_file_name")
	date_xcom = kwargs['ti'].xcom_pull(
		task_ids=f'ch_campaigns.update_data.campaigns_update.get_changed_campaigns',
		key="campaigns")
	filename_set = date_xcom.get('file_name')

	f_dict_df = files_iterable(filename_set, p, 'Campaigns', sql_tables, server_date_time)

	filename = f'all_changed_campaigns_{mask_file_name}.pkl'
	with open(f'{p}/{filename}', 'wb') as f:
		pickle.dump(f_dict_df, f)

	kwargs['task_instance'].xcom_push(
		key='update_campaigns',
		value={
			'file_name':filename,
			'campaigns_id': set(f_dict_df['Campaigns']['Id'])
		}
	)


def _add_changed_campaigns_to_postgres(**kwargs):
	url, headers, p = get_conf()
	date_xcom = kwargs['ti'].xcom_pull(
		task_ids='ch_campaigns.update_data.campaigns_update.separate_changed_campaigns',
		key='update_campaigns')
	with open(f'{p}/{date_xcom.get("file_name")}', 'rb') as f:
		dict_df = pickle.load(f)

	write_data_to_postgres(dict_df)


def _get_camp_id(**kwargs):
	connection, cursor = connect_db()
	request = """
		select s1."CampaignsId" from public."Campaigns.State" as s1
			JOIN
				(select "CampaignsId" as "id", max("ServerDateTime") as "dt" from public."Campaigns.State" 
				group by "CampaignsId") as s0
			ON s1."CampaignsId" = s0."id" AND s1."ServerDateTime" = s0."dt"
		where s1."State" = 'ON'
	"""
	time_df = pd.read_sql_query(con=connection, sql=request)
	connection.close()
	have_id = set(time_df['CampaignsId'].unique())

	date_xcom = kwargs['ti'].xcom_pull(
		task_ids='ch_campaigns.update_data.campaigns_update.separate_changed_campaigns',
		key="campaigns")

	if date_xcom is not None:
		have_campaingns_id = date_xcom.get('campaigns_id')
		have_id = have_id.difference(have_campaingns_id)
		if len(have_id) == 0:
			raise AirflowSkipException("No data to update")
	else:
		kwargs['task_instance'].xcom_push(
			key='company_id',
			value={
				'have_camp_id': have_id
			}
		)


def _get_ad_groups_id(**kwargs):
	connection, cursor = connect_db()
	request = """
		select s1."Id" from public."AdGroups.Status" as s1
		JOIN
			(select "Id", "Status", max("ServerDateTime") as "dt" from public."AdGroups.Status"
			group by "Id", "Status") as s0
			ON s1."Id" = s0."Id" AND s1."ServerDateTime" = s0."dt"
		where s1."Status" = 'ACCEPTED' or s1."Status" = 'PREACCEPTED'
	"""
	time_df = pd.read_sql_query(con=connection, sql=request)
	connection.close()
	have_id = set(time_df['Id'].unique())

	date_xcom = kwargs['ti'].xcom_pull(
		task_ids='ch_campaigns.update_data.adgroup_update.separate_adgroups_update_data',
		key="update_adgroups")
	if date_xcom is not None:
		have_adgroups_id = date_xcom.get('adgroups_id')
		have_id = have_id.difference(have_adgroups_id)
		if len(have_id) == 0:
			raise AirflowSkipException("No data to update")
	else:
		kwargs['task_instance'].xcom_push(
			key='groups_id',
			value={
				'have_ad_groups_id': have_id
			}
		)


def _get_ad_id(**kwargs):
	connection, cursor = connect_db()
	request = """
		select s1."Id" from public."Ads.State" as s1
		JOIN
			(select "Id", "State", max("ServerDateTime") as "dt" from public."Ads.State"
			group by "Id", "State") as s0
		ON s1."Id" = s0."Id" AND s1."ServerDateTime" = s0."dt"
		where s1."State" = 'ON'
	"""
	time_df = pd.read_sql_query(con=connection, sql=request)
	connection.close()

	have_id = set(time_df['Id'].unique())
	date_xcom = kwargs['ti'].xcom_pull(
		task_ids='ch_campaigns.update_data.ad_update.separate_ad_update_data',
		key="update_ads")
	if date_xcom is not None:
		have_ads_id = date_xcom.get('ads_id')
		have_id = have_id.difference(have_ads_id)
		if len(have_id) == 0:
			raise AirflowSkipException("No data to update")
	else:
		kwargs['task_instance'].xcom_push(
			key='ad_id',
			value={
				'have_ad_id': have_id
			}
		)


def _delete_files(**kwargs):
	del_list = list()

	date_xcom = kwargs['ti'].xcom_pull(
		task_ids='ch_campaigns.update_data.ad_update.get_ad_update',
		key="ads")
	if date_xcom is not None:
		del_list.append(date_xcom.get('file_name', None))

	date_xcom = kwargs['ti'].xcom_pull(
		task_ids='ch_campaigns.update_data.ad_update.separate_ad_update_data',
		key="update_ads")
	if date_xcom is not None:
		del_list.append(date_xcom.get('file_name', None))

	date_xcom = kwargs['ti'].xcom_pull(
		task_ids='ch_campaigns.update_data.adgroup_update.get_adgroup_update',
		key="adgroups")
	if date_xcom is not None:
		del_list.append(date_xcom.get('file_name', None))

	date_xcom = kwargs['ti'].xcom_pull(
		task_ids='ch_campaigns.update_data.adgroup_update.separate_adgroups_update_data',
		key="update_adgroups")
	if date_xcom is not None:
		del_list.append(date_xcom.get('file_name', None))

	date_xcom = kwargs['ti'].xcom_pull(
		task_ids='ch_campaigns.update_data.campaigns_update.get_changed_campaigns',
		key="campaigns")
	if date_xcom is not None:
		del_list.append(date_xcom.get('file_name', None))

	date_xcom = kwargs['ti'].xcom_pull(
		task_ids='ch_campaigns.update_data.campaigns_update.separate_changed_campaigns',
		key="update_campaigns")
	if date_xcom is not None:
		del_list.append(date_xcom.get('file_name', None))

	date_xcom = kwargs['ti'].xcom_pull(
		task_ids='ch_campaigns.update_data.check_campaigns',
		key="checkCampaigns")
	if date_xcom is not None:
		del_list.append(date_xcom.get('file_name', None))

	date_xcom = kwargs['ti'].xcom_pull(
		task_ids='ch_campaigns.update_data.pre_processing_check_campaigns',
		key="Check")
	if date_xcom is not None:
		del_list.append(date_xcom.get('SELF', None))
		del_list.append(date_xcom.get('CHILDREN', None))
		del_list.append(date_xcom.get('STAT', None))

	date_xcom = kwargs['ti'].xcom_pull(
		task_ids='ch_campaigns.update_data.get_changed_children',
		key="campaigns")
	if date_xcom is not None:
		del_list.append(date_xcom.get('file_name', None))

	date_xcom = kwargs['ti'].xcom_pull(
		task_ids='ch_campaigns.update_data.data_layout',
		key="processing")
	if date_xcom is not None:
		del_list.append(date_xcom.get('AdIds', None))
		del_list.append(date_xcom.get('AdGroupIds', None))

	del_cycle(del_list)




def _add_old_data(**kwargs):
	return
	date_xcom = kwargs['ti'].xcom_pull(task_ids=f'ch_campaigns.old_data.get_camp_id', key="company_id")
	camp_id = date_xcom.get('have_camp_id')
	date_xcom = kwargs['ti'].xcom_pull(task_ids=f'ch_campaigns.old_data.get_ad_groups_id', key="groups_id")
	groups_id = date_xcom.get('have_ad_groups_id')
	date_xcom = kwargs['ti'].xcom_pull(task_ids=f'ch_campaigns.old_data.get_ad_id', key="ad_id")
	ad_id = date_xcom.get('have_ad_id')
	sql_tables = dict(Variable.get("SQL_Tables_Schema", deserialize_json=True))
	return







def _get_audience_targets_update(**kwargs):
	date_xcom = kwargs['ti'].xcom_pull(task_ids='pre_processing_children_campaigns', key="processing")
	filename = date_xcom['AdGroupIds']
	date_xcom = kwargs['ti'].xcom_pull(task_ids=f'check_campaigns', key="checkCampaigns")
	mask_file_name = date_xcom.get("mask_file_name")

	payload = '''{
            "method": "get",
            "params": {
                "SelectionCriteria": {
                    "AdGroupIds": [
                        number
                    ]
                },
                "FieldNames": [
                    "Id",
                    "AdGroupId",
                    "CampaignId",
                    "RetargetingListId",
                    "InterestId",
                    "ContextBid",
                    "StrategyPriority",
                    "State"
                ]
            }
        }'''
	extention_url = 'audiencetargets'
	req_metod = "POST"
	file_name_mask = f'AudienceTargets_update_{mask_file_name}'

	filename_set = get_data_from_yandex(payload, extention_url, req_metod, filename, file_name_mask)

	if len(filename_set) == 0:
		raise AirflowSkipException("Нет данных AudienceTarget")

	kwargs['task_instance'].xcom_push(
		key='AudienceTargets',
		value={
			'file_name':filename_set
		}
	)


def _separate_audience_update_data(**kwargs):
	url, headers, p = get_conf()
	sql_tables = dict(Variable.get("SQL_Tables_Schema", deserialize_json=True))
	date_xcom = kwargs['ti'].xcom_pull(task_ids=f'audience_targets_update.get_audience_targets_update',
									   key="AudienceTargets")
	filename_set = date_xcom.get('file_name')
	date_xcom = kwargs['ti'].xcom_pull(task_ids=f'check_campaigns', key="checkCampaigns")
	server_date_time = date_xcom.get('server_date_time')
	date_xcom = kwargs['ti'].xcom_pull(task_ids=f'check_campaigns', key="checkCampaigns")
	mask_file_name = date_xcom.get("mask_file_name")

	f_dict_df = files_iterable(filename_set, p, 'AudienceTargets', sql_tables, server_date_time)

	filename = f'dict_audience_targets_update_{mask_file_name}.pkl'
	with open(f'{p}/{filename}', 'wb') as f:
		pickle.dump(f_dict_df, f)

	kwargs['task_instance'].xcom_push(
		key='audience_targets_update',
		value={
			'file_name':filename
		}
	)


def _add_audience_update_data_to_postgres(**kwargs):
	url, headers, p = get_conf()
	date_xcom = kwargs['ti'].xcom_pull(task_ids='audience_targets_update.separate_audience_update_data',
									   key='audience_targets_update')
	with open(f'{p}/{date_xcom.get("file_name")}', 'rb') as f:
		dict_df = pickle.load(f)

	write_data_to_postgres(dict_df)


def _get_keywords_update(**kwargs):
	date_xcom = kwargs['ti'].xcom_pull(task_ids='pre_processing_children_campaigns', key="processing")
	filename = date_xcom['AdGroupIds']
	date_xcom = kwargs['ti'].xcom_pull(task_ids=f'check_campaigns', key="checkCampaigns")
	mask_file_name = date_xcom.get("mask_file_name")

	payload = '''{
            "method": "get",
            "params": {
                "SelectionCriteria": {
            		"AdGroupIds": [number]
        		},
                "FieldNames": [
                    "Id",
                    "Keyword",
                    "State",
                    "Status",
                    "ServingStatus",
                    "AdGroupId",
                    "CampaignId",
                    "Bid",
                    "ContextBid",
                    "StrategyPriority",
                    "UserParam1",
                    "UserParam2",
                    "StatisticsSearch",
                    "StatisticsNetwork",
                    "AutotargetingCategories"
                ]
            }
        }'''
	extention_url = 'keywords'
	req_metod = "POST"
	file_name_mask = f'Keyword_update_{mask_file_name}_'

	filename_set = get_data_from_yandex(payload, extention_url, req_metod, filename, file_name_mask)

	if len(filename_set) == 0:
		raise AirflowSkipException("Нет данных Keyword")

	kwargs['task_instance'].xcom_push(
		key='keyword',
		value={
			'file_name':filename_set
		}
	)


def _separate_keywords_data_update(**kwargs):
	url, headers, p = get_conf()
	date_xcom = kwargs['ti'].xcom_pull(task_ids=f'check_campaigns', key="checkCampaigns")
	mask_file_name = date_xcom.get("mask_file_name")
	sql_tables = dict(Variable.get("SQL_Tables_Schema", deserialize_json=True))
	date_xcom = kwargs['ti'].xcom_pull(task_ids=f'keywords_update.get_keywords_update', key="keyword")
	filename_set = date_xcom.get('file_name')
	date_xcom = kwargs['ti'].xcom_pull(task_ids=f'check_campaigns', key="checkCampaigns")
	server_date_time = date_xcom.get('server_date_time')

	f_dict_df = files_iterable(filename_set, p, 'Keywords', sql_tables, server_date_time)

	filename = f'dict_keyword_update_{mask_file_name}.pkl'
	with open(f'{p}/{filename}', 'wb') as f:
		pickle.dump(f_dict_df, f)

	kwargs['task_instance'].xcom_push(
		key='keyword_update',
		value={
			'file_name':filename
		}
	)


def _add_keywords_update_to_postgres(**kwargs):
	url, headers, p = get_conf()
	date_xcom = kwargs['ti'].xcom_pull(task_ids='keywords_update.separate_keywords_data_update', key='keyword_update')
	with open(f'{p}/{date_xcom.get("file_name")}', 'rb') as f:
		dict_df = pickle.load(f)

	write_data_to_postgres(dict_df)


def _get_ad_extension_update(**kwargs):
	date_xcom = kwargs['ti'].xcom_pull(task_ids=f'check_campaigns', key="checkCampaigns")
	mask_file_name = date_xcom.get("mask_file_name")
	date_xcom = kwargs['ti'].xcom_pull(task_ids='pre_processing_children_campaigns', key="processing")
	filename = date_xcom['AdIds']

	if filename is None:
		raise AirflowSkipException("Нет обновления AdExtensions, т.к. нет AdIds для обновления")

	payload = '''{
              "method": "get",
              "params": {
                "SelectionCriteria": {
                  "Ids": [
                    number
                  ]
                },
                "FieldNames": [
                  "Id",
                  "Associated",
                  "Type",
                  "State",
                  "Status",
                  "StatusClarification"
                ],
                "CalloutFieldNames": [
                  "CalloutText"
                ]
              }
            }'''
	extention_url = 'adextensions'
	req_metod = "POST"
	file_name_mask = f'AdExtensions_update_{mask_file_name}_'

	filename_set = get_data_from_yandex(payload, extention_url, req_metod, filename, file_name_mask)

	if len(filename_set) == 0:
		raise AirflowSkipException("Нет данных AdExtensions")

	kwargs['task_instance'].xcom_push(
		key='AdExtensions',
		value={
			'file_name':filename_set
		}
	)


def _separate_ad_extension_update_data(**kwargs):
	url, headers, p = get_conf()
	pathlib.Path(p).mkdir(parents=True, exist_ok=True)
	date_xcom = kwargs['ti'].xcom_pull(task_ids=f'check_campaigns', key="checkCampaigns")
	mask_file_name = date_xcom.get("mask_file_name")
	sql_tables = dict(Variable.get("SQL_Tables_Schema", deserialize_json=True))
	date_xcom = kwargs['ti'].xcom_pull(task_ids=f'ad_extension_update.get_ad_extension_update', key="AdExtensions")
	filename_set = date_xcom.get('file_name')
	date_xcom = kwargs['ti'].xcom_pull(task_ids=f'check_campaigns', key="checkCampaigns")
	server_date_time = date_xcom.get('server_date_time')

	f_dict_df = files_iterable(filename_set, p, 'AdExtensions', sql_tables, server_date_time)

	filename = f'AdExtensions_update_{mask_file_name}.pkl'
	with open(f'{p}/{filename}', 'wb') as f:
		pickle.dump(f_dict_df, f)

	kwargs['task_instance'].xcom_push(
		key='AdExtensions',
		value={
			'file_name':filename
		}
	)


def _add_ad_extension_update_to_postgres(**kwargs):
	url, headers, p = get_conf()
	date_xcom = kwargs['ti'].xcom_pull(task_ids='ad_extension_update.separate_ad_extension_update_data',
									   key='AdExtensions')
	with open(f'{p}/{date_xcom.get("file_name")}', 'rb') as f:
		dict_df = pickle.load(f)

	write_data_to_postgres(dict_df)


def _get_site_links_set_update(**kwargs):
	date_xcom = kwargs['ti'].xcom_pull(task_ids='pre_processing_children_campaigns', key="processing")
	filename = date_xcom['AdIds']
	date_xcom = kwargs['ti'].xcom_pull(task_ids=f'check_campaigns', key="checkCampaigns")
	mask_file_name = date_xcom.get("mask_file_name")

	if filename is None:
		raise AirflowSkipException("Нет обновления SiteLinksSet, т.к. нет AdIds к обновлению")

	payload = '''{
              "method": "get",
              "params": {
                "SelectionCriteria": {
                  "Ids": [
                    number
                  ]
                },
                "FieldNames": [
                  "Id"
                ],
                "SitelinkFieldNames": [
                  "Title",
                  "Href",
                  "Description",
                  "TurboPageId"
                ]
              }
            }'''
	extention_url = 'sitelinks'
	req_metod = "POST"
	file_name_mask = f'SiteLinksSet_update_{mask_file_name}_'

	filename_set = get_data_from_yandex(payload, extention_url, req_metod, filename, file_name_mask)

	if len(filename_set) == 0:
		raise AirflowSkipException("Нет данных SiteLinksSet")

	kwargs['task_instance'].xcom_push(
		key='SiteLinksSet',
		value={
			'file_name':filename_set
		}
	)


def _separate_site_links_set_update_data(**kwargs):
	url, headers, p = get_conf()
	pathlib.Path(p).mkdir(parents=True, exist_ok=True)
	date_xcom = kwargs['ti'].xcom_pull(task_ids=f'check_campaigns', key="checkCampaigns")
	mask_file_name = date_xcom.get("mask_file_name")
	sql_tables = dict(Variable.get("SQL_Tables_Schema", deserialize_json=True))
	date_xcom = kwargs['ti'].xcom_pull(task_ids=f'site_links_set_update.get_site_links_set_update', key="SiteLinksSet")
	filename_set = date_xcom.get('file_name')
	date_xcom = kwargs['ti'].xcom_pull(task_ids=f'check_campaigns', key="checkCampaigns")
	server_date_time = date_xcom.get('server_date_time')

	f_dict_df = files_iterable(filename_set, p, 'SitelinksSets', sql_tables, server_date_time)

	filename = f'SiteLinksSet_update_{mask_file_name}.pkl'
	with open(f'{p}/{filename}', 'wb') as f:
		pickle.dump(f_dict_df, f)

	kwargs['task_instance'].xcom_push(
		key='SiteLinksSet',
		value={
			'file_name':filename
		}
	)


def _add_site_links_set_update_to_postgres(**kwargs):
	url, headers, p = get_conf()
	date_xcom = kwargs['ti'].xcom_pull(task_ids='site_links_set_update.separate_site_links_set_update_data',
									   key='SiteLinksSet')
	with open(f'{p}/{date_xcom.get("file_name")}', 'rb') as f:
		dict_df = pickle.load(f)

	write_data_to_postgres(dict_df)


def _join_data(**kwargs):
	date_xcom = kwargs['ti'].xcom_pull(task_ids=f'check_campaigns', key="checkCampaigns")
	mask_file_name = date_xcom.get("mask_file_name")
	date_xcom = kwargs['ti'].xcom_pull(task_ids='pre_processing_children_campaigns', key="processing")
	url, headers, p = get_conf()
	pathlib.Path(p).mkdir(parents=True, exist_ok=True)
	united_compaign_id_set = set()

	filename = date_xcom['CampaignId']
	if filename is not None:
		with open(f'{p}/{filename}', 'rb') as f:
			campaign_id_set = pickle.load(f)
	else:
		campaign_id_set = set()

	date_xcom = kwargs['ti'].xcom_pull(task_ids=f'pre_processing_check_campaigns', key="Check")
	filename = date_xcom['STAT']
	if filename is not None:
		with open(f'{p}/{filename}', 'rb') as f:
			stat_set = pickle.load(f)
	else:
		stat_set = set()

	date_xcom = kwargs['ti'].xcom_pull(task_ids=f'pre_processing_check_campaigns', key="Check")
	filename = date_xcom['SELF']
	if filename is not None:
		with open(f'{p}/{filename}', 'rb') as f:
			self_set = pickle.load(f)
	else:
		self_set = set()

	united_compaign_id_set.update(campaign_id_set, stat_set, self_set)
	filename = f'united_compaign_set_{mask_file_name}.pkl'
	with open(f'{p}/{filename}', 'wb') as f:
		pickle.dump(united_compaign_id_set, f)

	kwargs['task_instance'].xcom_push(
		key='united_compaign',
		value={
			'file_name':filename
		}
	)


def _check_change_dictionaries(**kwargs):
	date_xcom = kwargs['ti'].xcom_pull(task_ids=f'first_branching', key="first_branch")
	server_time = date_xcom['server_time']
	url, headers, p = get_conf()
	url = url + 'changes'
	payload = json.dumps({
		"method":"checkDictionaries",
		"params":{
			"Timestamp":f"{server_time}"
		}
	})
	try:
		response = requests.request("POST", url, headers=headers, data=payload)
	except AirflowWebServerTimeout as err:
		print('AirflowWebServerTimeout', err)
	else:
		get_logs = json.loads(response.text)
		if response.status_code == 200:
			get_logs = request_error(get_logs)
			kwargs['task_instance'].xcom_push(
				key='change_dictionaries',
				value={
					'change_dict':get_logs['result']
				}
			)


def _update_dictionaries(**kwargs):
	date_xcom = kwargs['ti'].xcom_pull(task_ids=f'check_campaigns', key="checkCampaigns")
	mask_file_name = date_xcom.get("mask_file_name")
	date_xcom = kwargs['ti'].xcom_pull(task_ids=f'update_dict.check_change_dictionaries', key="change_dictionaries")
	check_dict = date_xcom['change_dict']
	for key in check_dict.keys():
		if key == 'Timestamp':
			kwargs['task_instance'].xcom_push(
				key='new_server_time',
				value={
					'server_time':check_dict.get(key)
				}
			)
		elif check_dict.get(key) == 'YES':
			print(f'Нужны изминения для: {key}')
		else:
			print(f'Нет изминений в: {key}')


def _update_server_time(**kwargs):
	date_xcom = kwargs['ti'].xcom_pull(task_ids=f'check_campaigns', key="checkCampaigns")
	new_server_time = date_xcom.get('server_date_time')
	date_xcom = kwargs['ti'].xcom_pull(task_ids=f'first_branching', key="first_branch")
	old_server_time = date_xcom['server_time']
	connection, cursor = connect_db()
	table_name = '"ServerTime"'
	request = f"UPDATE {table_name} set dt = '{new_server_time}' where dt = '{old_server_time}'"
	cursor.execute(request)
	connection.commit()
	cursor.close()
	connection.close()


def _delete_first_upload_files(**kwargs):
	del_list = list()

	date_xcom = kwargs['ti'].xcom_pull(task_ids='full_load.campaign.get_campaigns', key='campaigns')
	if date_xcom is not None:
		del_list.append(date_xcom.get("file_name", None))

	date_xcom = kwargs['ti'].xcom_pull(task_ids='full_load.campaign.separate_campaigns_data', key='companies_df')
	if date_xcom is not None:
		del_list.append(date_xcom.get("companies_df", None))

	date_xcom = kwargs['ti'].xcom_pull(task_ids=f'full_load.download_dict.get_dictionaries', key='dictionaries')
	if date_xcom is not None:
		del_list.append(date_xcom.get("file_name", None))

	date_xcom = kwargs['ti'].xcom_pull(task_ids=f'full_load.download_dict.separate_dictionaries',
									   key="sep_dictionaries")
	if date_xcom is not None:
		del_list.append(date_xcom.get("file_name", None))

	date_xcom = kwargs['ti'].xcom_pull(task_ids='full_load.keyword.get_keywords', key='keyword')
	if date_xcom is not None:
		del_list.append(date_xcom.get("file_name", None))

	date_xcom = kwargs['ti'].xcom_pull(task_ids='full_load.keyword.separate_keywords_data', key='all_keyword')
	if date_xcom is not None:
		del_list.append(date_xcom.get("file_name", None))

	date_xcom = kwargs['ti'].xcom_pull(task_ids=f'full_load.audience_targets.get_audience_targets',
									   key="AudienceTargets")
	if date_xcom is not None:
		del_list.append(date_xcom.get("file_name", None))

	date_xcom = kwargs['ti'].xcom_pull(task_ids='full_load.audience_targets.separate_audience_data',
									   key='all_audience_targets')
	if date_xcom is not None:
		del_list.append(date_xcom.get("file_name", None))

	date_xcom = kwargs['ti'].xcom_pull(task_ids=f'full_load.site_links.get_SiteLinksSet', key="SiteLinksSet")
	if date_xcom is not None:
		del_list.append(date_xcom.get("file_name", None))

	date_xcom = kwargs['ti'].xcom_pull(task_ids='full_load.site_links.separate_SiteLinksSet', key='SiteLinksSet')
	if date_xcom is not None:
		del_list.append(date_xcom.get("file_name", None))

	date_xcom = kwargs['ti'].xcom_pull(task_ids=f'full_load.ad_extension.get_AdExtension', key="AdExtensions")
	if date_xcom is not None:
		del_list.append(date_xcom.get("file_name", None))

	date_xcom = kwargs['ti'].xcom_pull(task_ids='full_load.ad_extension.separate_AdExtension_data', key='AdExtension')
	if date_xcom is not None:
		del_list.append(date_xcom.get("file_name", None))

	date_xcom = kwargs['ti'].xcom_pull(task_ids=f'full_load.adgroup.get_adgroup', key="adgroups")
	if date_xcom is not None:
		del_list.append(date_xcom.get("all_adgroups", None))

	date_xcom = kwargs['ti'].xcom_pull(task_ids='full_load.adgroup.separate_adgroups_data', key='all_adgroups')
	if date_xcom is not None:
		del_list.append(date_xcom.get("file_name", None))

	date_xcom = kwargs['ti'].xcom_pull(task_ids=f'full_load.ad.get_ad', key="ads")
	if date_xcom is not None:
		del_list.append(date_xcom.get("all_ads", None))

	date_xcom = kwargs['ti'].xcom_pull(task_ids='full_load.ad.separate_ad_data', key='all_ads')
	if date_xcom is not None:
		del_list.append(date_xcom.get("file_name", None))

	date_xcom = kwargs['ti'].xcom_pull(task_ids='full_load.check_campaigns', key="checkCampaigns")
	if date_xcom is not None:
		del_list.append(date_xcom.get('file_name', None))

	del_cycle(del_list)


def _create_pipeline(dag_, start_dt):
	get_server_time = PythonOperator(
		task_id='get_server_time',
		python_callable=_get_server_time,
		dag=dag_)

	# Код для функций первой загрузки
	first_branch = BranchPythonOperator(
		task_id='its_first_start',
		python_callable=_first_branch,
		op_kwargs={'start_date':start_dt},
		dag=dag_)

	with TaskGroup("full_load", tooltip="Full load of all companies") as full_load:
		with TaskGroup("site_links", tooltip="get SiteLinksSet") as site_links:
			get_site_links_set = PythonOperator(
				task_id='get_SiteLinksSet',
				python_callable=_get_site_links_set,
				dag=dag_)

			separate_site_links_set_data = PythonOperator(
				task_id='separate_SiteLinksSet',
				python_callable=_separate_site_links_set_data,
				dag=dag_)

			add_site_links_set_to_postgres = PythonOperator(
				task_id='add_SiteLinksSet_to_postgres',
				python_callable=_add_site_links_set_to_postgres,
				dag=dag_)

			get_site_links_set >> separate_site_links_set_data >> add_site_links_set_to_postgres

		with TaskGroup("ad_extension", tooltip="get AdExtension") as ad_extension:
			get_ad_extension = PythonOperator(
				task_id='get_AdExtension',
				python_callable=_get_ad_extension,
				dag=dag_)

			separate_adextension_data = PythonOperator(
				task_id='separate_AdExtension_data',
				python_callable=_separate_adextension_data,
				dag=dag_)

			add_adextension_to_postgres = PythonOperator(
				task_id='add_AdExtension_to_postgres',
				python_callable=_add_adextension_to_postgres,
				dag=dag_)

			get_ad_extension >> separate_adextension_data >> add_adextension_to_postgres

		with TaskGroup("download_dict", tooltip="get Dictionaries") as download_dict:
			get_dictionaries = PythonOperator(
				task_id='get_dictionaries',
				python_callable=_get_dictionaries,
				dag=dag_)

			separate_dictionaries = PythonOperator(
				task_id='separate_dictionaries',
				python_callable=_separate_dictionaries,
				dag=dag_)

			add_dictionaries_to_bd = PythonOperator(
				task_id='add_dictionaries_to_bd',
				python_callable=_add_dictionaries_to_bd,
				dag=dag_)

			get_dictionaries >> separate_dictionaries >> add_dictionaries_to_bd

		with TaskGroup("audience_targets", tooltip="get AudienceTarget") as audience_targets:
			get_audience_targets = PythonOperator(
				task_id='get_audience_targets',
				python_callable=_get_audience_targets,
				dag=dag_)

			separate_audience_data = PythonOperator(
				task_id='separate_audience_data',
				python_callable=_separate_audience_data,
				dag=dag_)

			add_audience_data_to_postgres = PythonOperator(
				task_id='add_audience_data_to_postgres',
				python_callable=_add_audience_data_to_postgres,
				dag=dag_)

			get_audience_targets >> separate_audience_data >> add_audience_data_to_postgres

		with TaskGroup("keyword", tooltip="get Keyword") as keyword:
			get_keywords = PythonOperator(
				task_id='get_keywords',
				python_callable=_get_keywords,
				dag=dag_)

			separate_keywords_data = PythonOperator(
				task_id='separate_keywords_data',
				python_callable=_separate_keywords_data,
				dag=dag_)

			add_keywords_to_postgres = PythonOperator(
				task_id='add_keywords_to_postgres',
				python_callable=_add_keywords_to_postgres,
				dag=dag_)

			get_keywords >> separate_keywords_data >> add_keywords_to_postgres

		with TaskGroup("ad", tooltip="get Ad") as ad:
			get_ad = PythonOperator(
				task_id='get_ad',
				python_callable=_get_ad,
				dag=dag_)

			separate_ad_data = PythonOperator(
				task_id='separate_ad_data',
				python_callable=_separate_ad_data,
				dag=dag_)

			add_ad_to_postgres = PythonOperator(
				task_id='add_ad_to_postgres',
				python_callable=_add_ad_to_postgres,
				dag=dag_)

			get_ad >> separate_ad_data >> add_ad_to_postgres

		with TaskGroup("adgroup", tooltip="get AdGroup") as adgroup:
			get_adgroup = PythonOperator(
				task_id='get_adgroup',
				python_callable=_get_adgroup,
				dag=dag_)

			separate_adgroups_data = PythonOperator(
				task_id='separate_adgroups_data',
				python_callable=_separate_adgroups_data,
				dag=dag_)

			add_adgroups_to_postgres = PythonOperator(
				task_id='add_adgroups_to_postgres',
				python_callable=_add_adgroups_to_postgres,
				dag=dag_)

			get_adgroup >> separate_adgroups_data >> add_adgroups_to_postgres

		with TaskGroup("campaign", tooltip="get Campaign") as campaign:
			get_campaigns = PythonOperator(
				task_id='get_campaigns',
				python_callable=_get_campaigns,
				dag=dag_)

			separate_campaigns_data = PythonOperator(
				task_id='separate_campaigns_data',
				python_callable=_separate_campaigns_data,
				dag=dag_)

			add_companies_to_postgres = PythonOperator(
				task_id='add_companies_to_postgres',
				python_callable=_add_companies_to_postgres,
				dag=dag_)

			get_campaigns >> separate_campaigns_data >> add_companies_to_postgres

		create_table = PythonOperator(
			task_id='create_table',
			python_callable=_create_table,
			dag=dag_)

		add_server_time_to_bd = PythonOperator(
			task_id='add_server_time_to_bd',
			python_callable=_add_server_time_to_bd,
			dag=dag_)

		delete_first_upload_files = PythonOperator(
			task_id='delete_first_upload_files',
			python_callable=_delete_first_upload_files,
			dag=dag_)

		create_table >> [site_links, ad_extension, download_dict, ad, campaign]
		separate_campaigns_data >> [keyword, audience_targets, adgroup]
		separate_adgroups_data >> ad
		[campaign, adgroup, ad, keyword, audience_targets] >> add_server_time_to_bd >> delete_first_upload_files
		[download_dict, ad_extension, site_links] >> add_server_time_to_bd >> delete_first_upload_files

	with TaskGroup("ch_campaigns", tooltip="Checking for changes in campaigns") as ch_campaigns:

		with TaskGroup("update_data", tooltip="Update data") as update_data:

			with TaskGroup("adgroup_update", tooltip="AdGroup update") as adgroup_update:
				get_adgroup_update = PythonOperator(
					task_id='get_adgroup_update',
					python_callable=_get_adgroup_update,
					dag=dag_)

				separate_adgroups_update_data = PythonOperator(
					task_id='separate_adgroups_update_data',
					python_callable=_separate_adgroups_update_data,
					dag=dag_)

				add_adgroups_update_to_postgres = PythonOperator(
					task_id='add_adgroups_update_to_postgres',
					python_callable=_add_adgroups_update_to_postgres,
					dag=dag_)

				get_adgroup_update >> separate_adgroups_update_data >> add_adgroups_update_to_postgres

			with TaskGroup("ad_update", tooltip="Ad update") as ad_update:
				get_ad_update = PythonOperator(
					task_id='get_ad_update',
					python_callable=_get_ad_update,
					dag=dag_)

				separate_ad_update_data = PythonOperator(
					task_id='separate_ad_update_data',
					python_callable=_separate_ad_update_data,
					dag=dag_)

				add_ad_update_to_postgres = PythonOperator(
					task_id='add_ad_update_to_postgres',
					python_callable=_add_ad_update_to_postgres,
					dag=dag_)

				get_ad_update >> separate_ad_update_data >> add_ad_update_to_postgres

			with TaskGroup("campaigns_update", tooltip="campaigns update") as campaigns_update:
				get_changed_campaigns = PythonOperator(
					task_id='get_changed_campaigns',
					python_callable=_get_changed_campaigns,
					dag=dag_)

				separate_changed_campaigns = PythonOperator(
					task_id='separate_changed_campaigns',
					python_callable=_separate_changed_campaigns,
					dag=dag_)

				add_changed_campaigns_to_postgres = PythonOperator(
					task_id='add_changed_campaigns_to_postgres',
					python_callable=_add_changed_campaigns_to_postgres,
					dag=dag_)

				get_changed_campaigns >> separate_changed_campaigns >> add_changed_campaigns_to_postgres

			check_campaigns = PythonOperator(
				task_id='check_campaigns',
				python_callable=_check_campaigns,
				dag=dag_)

			pre_processing_check_campaigns = PythonOperator(
				task_id='pre_processing_check_campaigns',
				python_callable=_pre_processing_check_campaigns,
				dag=dag_)

			get_changed_children = PythonOperator(
				task_id='get_changed_children',
				python_callable=_get_changed_children,
				dag=dag_)

			data_layout = PythonOperator(
				task_id='data_layout',
				python_callable=_data_layout,
				dag=dag_)

			join_data = DummyOperator(
				task_id='join_data',
				trigger_rule='none_failed',
				dag=dag_)

			delete_files = PythonOperator(
				task_id='delete_files',
				python_callable=_delete_files,
				dag=dag_)

			check_campaigns >> pre_processing_check_campaigns >> get_changed_children >> data_layout
			data_layout >> [adgroup_update, ad_update] >> join_data
			campaigns_update >> join_data >> delete_files

		with TaskGroup("old_data", tooltip="Old data") as old_data:

			get_camp_id = PythonOperator(
				task_id='get_camp_id',
				python_callable=_get_camp_id,
				dag=dag_)

			get_ad_groups_id = PythonOperator(
				task_id='get_ad_groups_id',
				python_callable=_get_ad_groups_id,
				dag=dag_)

			get_ad_id = PythonOperator(
				task_id='get_ad_id',
				python_callable=_get_ad_id,
				dag=dag_)

			add_old_data = PythonOperator(
				task_id='add_old_data',
				python_callable=_add_old_data,
				trigger_rule="none_failed",
				dag=dag_)


			[get_camp_id, get_ad_groups_id, get_ad_id] >>  add_old_data

		# Код для функций обновления
		python_sensor = PythonSensor(
			task_id=f'python_sensor',
			python_callable=_python_sensor,
			mode='reschedule',
			dag=dag_)


		python_sensor >> [old_data, campaigns_update]
		python_sensor >> update_data
		pre_processing_check_campaigns >> campaigns_update
		update_data >> old_data
	"""
    

    with TaskGroup("site_links_set_update", tooltip="SiteLinksSet update") as site_links_set_update:

        get_site_links_set_update = PythonOperator(
            task_id='get_site_links_set_update',
            python_callable=_get_site_links_set_update,
            dag=dag_)

        separate_site_links_set_update_data = PythonOperator(
            task_id='separate_site_links_set_update_data',
            python_callable=_separate_site_links_set_update_data,
            dag=dag_)

        add_site_links_set_update_to_postgres = PythonOperator(
            task_id='add_site_links_set_update_to_postgres',
            python_callable=_add_site_links_set_update_to_postgres,
            dag=dag_)

        get_site_links_set_update >> separate_site_links_set_update_data >> add_site_links_set_update_to_postgres

    with TaskGroup("keywords_update", tooltip="Keyword update") as keywords_update:

        get_keywords_update = PythonOperator(
            task_id='get_keywords_update',
            python_callable=_get_keywords_update,
            dag=dag_)

        separate_keywords_data_update = PythonOperator(
            task_id='separate_keywords_data_update',
            python_callable=_separate_keywords_data_update,
            dag=dag_)

        add_keywords_update_to_postgres = PythonOperator(
            task_id='add_keywords_update_to_postgres',
            python_callable=_add_keywords_update_to_postgres,
            dag=dag_)

        get_keywords_update >> separate_keywords_data_update >> add_keywords_update_to_postgres

    with TaskGroup("audience_targets_update", tooltip="AudianceTarget update") as audience_targets_update:

        get_audience_targets_update = PythonOperator(
            task_id='get_audience_targets_update',
            python_callable=_get_audience_targets_update,
            dag=dag_)

        separate_audience_update_data = PythonOperator(
            task_id='separate_audience_update_data',
            python_callable=_separate_audience_update_data,
            dag=dag_)

        add_audience_update_data_to_postgres = PythonOperator(
            task_id='add_audience_update_data_to_postgres',
            python_callable=_add_audience_update_data_to_postgres,
            dag=dag_)

        get_audience_targets_update >> separate_audience_update_data >> add_audience_update_data_to_postgres

    with TaskGroup("ad_extension_update", tooltip="AdExtension update") as ad_extension_update:
        get_ad_extension_update = PythonOperator(
            task_id='get_ad_extension_update',
            python_callable=_get_ad_extension_update,
            dag=dag_)

        separate_ad_extension_update_data = PythonOperator(
            task_id='separate_ad_extension_update_data',
            python_callable=_separate_ad_extension_update_data,
            dag=dag_)

        add_ad_extension_update_to_postgres = PythonOperator(
            task_id='add_ad_extension_update_to_postgres',
            python_callable=_add_ad_extension_update_to_postgres,
            dag=dag_)

        get_ad_extension_update >> separate_ad_extension_update_data >> add_ad_extension_update_to_postgres

    join_data = PythonOperator(
        task_id='join_data',
        python_callable=_join_data,
        dag=dag_)

    serd_join_branch = DummyOperator(
        task_id='serd_join_branch',
        trigger_rule='none_failed',
        dag=dag_)

 
    with TaskGroup("adgroup_update", tooltip="AdGroup update") as adgroup_update:

        get_adgroup_update = PythonOperator(
            task_id='get_adgroup_update',
            python_callable=_get_adgroup_update,
            dag=dag_)

        separate_adgroups_update_data = PythonOperator(
            task_id='separate_adgroups_update_data',
            python_callable=_separate_adgroups_update_data,
            dag=dag_)



        get_adgroup_update >> separate_adgroups_update_data >> add_adgroups_update_to_postgres

    first_join_branch = DummyOperator(
        task_id='first_join_branch',
        trigger_rule='none_failed',
        dag=dag_)





    second_join_branch = DummyOperator(
        task_id='second_join_branch',
        trigger_rule='none_failed',
        dag=dag_)

    with TaskGroup("update_dict", tooltip="update Dictionaries") as update_dict:

        check_change_dictionaries = PythonOperator(
            task_id='check_change_dictionaries',
            python_callable=_check_change_dictionaries,
            dag=dag_)

        update_dictionaries = PythonOperator(
            task_id='update_dictionaries',
            python_callable=_update_dictionaries,
            dag=dag_)

        check_change_dictionaries >> update_dictionaries

    update_server_time = PythonOperator(
        task_id='update_server_time',
        python_callable=_update_server_time,
        dag=dag_)


    """

	# Блок функций первой загрузки
	#    create_table >> first_branch
	get_server_time >> first_branch >> [ch_campaigns, full_load]

	"""   
    check_campaigns >> update_dict
    get_server_time >> [download_dict, campaign, ad_extension, site_links]
    separate_campaigns_data >> [adgroup, keyword, audience_targets]
    separate_adgroups_data >> ad
    ad_extension >> add_ad_to_postgres
    site_links >> add_ad_to_postgres
    download_dict >> add_server_time_to_bd
    keyword >> add_server_time_to_bd
    audience_targets >> add_server_time_to_bd
    adgroup >> add_server_time_to_bd
    ad >> add_server_time_to_bd
    campaign >> add_server_time_to_bd
    add_server_time_to_bd >> delete_first_upload_files

    # Блок функций обновления данных
    check_campaigns >> pre_processing_check_campaigns >> [get_changed_children_campaigns, join_data,]
    get_changed_children_campaigns >> pre_processing_children_campaigns >> [adgroup_update,
                                                                            ad_update,
                                                                            join_data,
                                                                            site_links_set_update,
                                                                            ad_extension_update,
                                                                            keywords_update,
                                                                            audience_targets_update]
    audience_targets_update >> serd_join_branch
    keywords_update >> serd_join_branch
    ad_extension_update >> serd_join_branch >> add_ad_update_to_postgres
    site_links_set_update >> serd_join_branch >> second_join_branch
    join_data >> campaigns_update
    campaigns_update >> adgroup_update >> first_join_branch >> second_join_branch
    first_join_branch >> add_ad_update_to_postgres
    update_dict >> update_server_time
    ad_update >> second_join_branch >> update_server_time >> delete_files
    """


start_date = pendulum.datetime(2022, 6, 11)

with DAG(
		dag_id='YandexDirect',
		# schedule_interval = '@daily',
		# schedule_interval='@once',
		schedule_interval='@hourly',
		catchup=False,
		start_date=start_date,
		template_searchpath='opt/airflow/local_volume/',
		render_template_as_native_obj=True) as dag:
	_create_pipeline(dag, start_date)
