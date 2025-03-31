from src.create_dim_tables import make_dim_tables,set_up_dims_table,put_info_into_dims_schema
from src.connection import connect_to_db, close_db_connection
from src.create_raw_tables import make_raw_tables
import pytest
from unittest import mock
from src.dim_date_function import extract_date_info_from_dim_date
from src.get_currency_name import get_currency_details

# Table headers defined at bottom of file

test_db = connect_to_db()

result = test_db.run("SELECT current_database();")
print(f"Raw query result : {result}")
database_name = result[0][0]
print(f"Connected to database: {database_name}")

@pytest.fixture 
def date_id():
    return "2022-11-03T14:20:51.563000"
 

@pytest.fixture
def currency_id():
    return 1

@pytest.fixture
def mock_db_connection():
    mock_db = mock.MagicMock()
    return mock_db


class TestSetUpDimsTable:

    def test_six_tables_created(self):
        test_db = connect_to_db()
        make_raw_tables(lambda_local_db)
        test_input = ["address","counterparty","currency","department","design","sales_order","staff"]
        make_dim_tables(test_db)

        for table in dim_tables_column_headers:
            test_db.run(f"SELECT * FROM {table};")
            column_headers = [c['name'] for c in test_db.columns]
            assert column_headers == dim_tables_column_headers[table]
        close_db_connection(test_db)


    def test_tables_are_empty(self):
        test_db = connect_to_db()
        test_input = ["address","counterparty","currency","department","design","sales_order","staff"]
        make_dim_tables(test_db)

        for table in dim_tables_column_headers:
            result = test_db.run(f"SELECT * FROM {table};")
            assert result == []
        close_db_connection(test_db)


class TestPutInfontoDimsSchema():
    # test when incorrect table provided
    def test_no_data_provided(self):
        with pytest.raises(Exception,match="No rows outputted"):
            test_input = []
            test_db = connect_to_db()
            make_dim_tables(test_db)
        close_db_connection(test_db)

    def test_wrong_name_provided(self):
        with pytest.raises(Exception,match="Dimension table names requested are not valid"):
            test_input = ["test_wrong_name"]
            test_db = connect_to_db()
            make_dim_tables(test_db)
        close_db_connection(test_db)

    def test_insertion_for_one_table(self):

        test_input = {"staff":[
        {"staff_id": 1,
         "first_name": "Jeremie",
         "last_name": "Franey",
         "department_id": 1,
         "email_address": "jeremie.franey@terrifictotes.com",
         "created_at": "2022-11-03T14:20:51.563000",
         "last_updated": "2022-11-03T14:20:51.563000"}],
         "department": [
        {"department_id": 1,
         "department_name": "Sales",
         "location": "Manchester",
         "manager": "Richard Roma",
         "created_at": "2022-11-03T14:20:49.962000",
         "last_updated": "2022-11-03T14:20:49.962000"
        }]}
        test_db = connect_to_db()
        func_result = make_temporary_tables(test_db,test_input)
        table_names = func_result[0]
        result = make_dim_tables(test_db)
        assert result["dim_staff"] == [[1, 'Jeremie', 'Franey', 'Sales', 'Manchester', 'jeremie.franey@terrifictotes.com']]
        close_db_connection(test_db)            

    def test_insert_table_with_empty_cell(self):
        test_input = {"staff":[
        {"staff_id": 1,
         "first_name": "Jeremie",
         "last_name": "Franey",
         "department_id": 1,
         "email_address": "jeremie.franey@terrifictotes.com",
         "created_at": "2022-11-03T14:20:51.563000",
         "last_updated": "2022-11-03T14:20:51.563000"}],
         "department": [
        {"department_id": 1,
         "department_name": "Sales",
         "location": "Manchester",
         "manager": "Richard Roma",
         "created_at": "2022-11-03T14:20:49.962000",
         "last_updated": "2022-11-03T14:20:49.962000"
        }]}

        test_db = connect_to_db()

        func_result = make_temporary_tables(test_db, test_input)
        table_names = func_result[0]
        result = make_dim_tables(test_db)
        assert result["dim_staff"] == [[1, 'Jeremie', 'Franey', 'Sales', 'Manchester', 'jeremie.franey@terrifictotes.com']]

        close_db_connection(test_db)

    def test_no_join_between_data(self):
        with pytest.raises(Exception,match="No paired keys to perform JOIN on"):
            test_input = {"staff":[
            {"staff_id": 1,
            "first_name": "Jeremie",
            "last_name": "Franey",
            "department_id": 1,
            "email_address": "jeremie.franey@terrifictotes.com",
            "created_at": "2022-11-03T14:20:51.563000",
            "last_updated": "2022-11-03T14:20:51.563000"}],
            "department": [
            {"department_id": 2,
            "department_name": "Sales",
            "location": "Manchester",
            "manager": "Richard Roma",
            "created_at": "2022-11-03T14:20:49.962000",
            "last_updated": "2022-11-03T14:20:49.962000"
            }]}
            test_db = connect_to_db()
            func_result = make_temporary_tables(test_db,test_input)
            table_names = func_result[0]
            make_dim_tables(test_db)
            close_db_connection(test_db)

# Mocked database connection 
@pytest.fixture
def mock_db_connection():
    mock_db = mock.Mock()
    return mock_db

def test_process_dim_date(mock_db_connection, date_id):    
    # Mock the function 'extract_date_info_from_dim_date' 
    with mock.patch('src.create_dim_tables.extract_date_info_from_dim_date') as mock_extract_date:
        
        mock_extract_date.return_value = {'day': 3, 'month': 11, 'year': 2022} 

        date_id = '2022-11-03T14:20:51.563000' 
        currency_id = 1
        dim_tables_created = ["dim_date", "dim_currency"]   
        # Mocks database connection
        mock_db_connection.run.return_value = [{"result": "success" }]

        result = make_dim_tables(mock_db_connection)

        print(f"mock_extract_date.call_count: {mock_extract_date.call_count}")
        print("mock called", mock_extract_date.call_args_list)
        print(f"Result: {result}")

        mock_extract_date.assert_called_once_with(date_id)
        print(mock_extract_date.return_value)

        assert 'dim_date' in result
        assert result['dim_date'][0]['result'] == "success"

        print(f"dim_date: {result}")


def test_process_dim_currency(mock_db_connection, currency_id):
    
    # Mock the function 'get_currency_details' 
    with mock.patch('src.create_dim_tables.get_currency_details') as mock_get_currency:
        mock_get_currency.return_value = {"currency_code": "GBP", "currency_name": "British pound sterling"}  

        dim_tables_created = ["dim_currency", "dim_date"]  
        currency_id = 1 
        date_id = '2022-11-03T14:20:51.563000'

        mock_db_connection.run.return_value = [{"result": "success"}]
        
        result = make_dim_tables(mock_db_connection)

        print(f"mock_get_currency.call_count: {mock_get_currency.call_count}")

        mock_get_currency.assert_called_once_with(currency_id)

        assert 'dim_currency' in result
        assert result['dim_currency'][0]['result'] == 'success'
        print(f"dim_currency: {result}")


# Table headers
dim_tables_column_headers = {"dim_date":
  ["date_id",
  "year",
  "month",
  "day",
  "day_of_week",
  "day_name",
  "month_name",
  "quarter"],

  "dim_staff":
  ["staff_id",
  "first_name",
  "last_name" ,
  "department_name",
  "location",
  "email_address"],

  "dim_location":
  ["location_id",
  "address_line_1",
  "address_line_2",
  "district",
  "city",
  "postal_code",
  "country",
  "phone"],

  "dim_currency":
  ["currency_id",
  "currency_code",
  "currency_name"],


  "dim_design":
  ["design_id",
  "design_name",
  "file_location",
  "file_name"],

  "dim_counterparty":
  ["counterparty_id",
  "counterparty_legal_name",
  "counterparty_legal_address_line_1",
  "counterparty_legal_address_line_2",
  "counterparty_legal_district",
  "counterparty_legal_city",
  "counterparty_legal_postal_code",
  "counterparty_legal_country",
  "counterparty_legal_phone_number"]
}