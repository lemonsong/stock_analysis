from pathlib import Path
import sys

def get_file_paths_pathlib(folder_path: str) -> list[str]:
    """
    Returns a list of full file paths (as strings) for all files
    directly within the specified folder.
    """
    folder = Path(folder_path)

    # List comprehension to iterate, filter for files, and convert to string
    file_paths = [
        str(p.resolve())  # .resolve() gets the absolute path, str() converts Path object to string
        for p in folder.iterdir()
        # if p.is_file()
        if p.suffix == ".csv" # only get csv file
    ]
    return file_paths

# def extract_stock_symbol_from_path(file_path, from_format):
#     symbol_name = file_path.split('/')[-1].split('.')[0]
#     if from_format == 'MARKETnumber':
#         return symbol_name
#     elif from_format == 'number_MARKET':
#         symbol_name_splited = symbol_name.split('_')
#         return symbol_name_splited[1] + symbol_name_splited[0]
#     else:
#         print('The from_format input does not exist')
#         sys.exit()

def format_stock_symbol(symbol_name, from_format, to_format):
    if from_format == 'MARKETnumber':
        if to_format == 'MARKETnumber':
            return symbol_name
        elif to_format == 'number.MARKET':
            return symbol_name[-6:]+'.'+symbol_name[:2]
        else:
            print('The to_format input does not exist')
            sys.exit()
    elif from_format == 'number_MARKET':
        symbol_name_splited = symbol_name.split('_')
        if to_format == 'number.MARKET':
            return symbol_name_splited[0] + '.' + symbol_name_splited[1]
        elif to_format == 'MARKETnumber':
            return symbol_name_splited[1] + symbol_name_splited[0]
        else:
            print('The to_format input does not exist')
            sys.exit()
    elif from_format == 'number.MARKET':
        symbol_name_splited = symbol_name.split('.')
        if to_format == 'MARKETnumber':
            return symbol_name_splited[1] + symbol_name_splited[0]
        else:
            print('The to_format input does not exist')
            sys.exit()

    elif from_format == 'MARKETnumber_xxx':
        if to_format == 'MARKETnumber':
            return symbol_name.split('_')[0]
        else:
            print('The to_format input does not exist')
            sys.exit()
    else:
        print('The from_format input does not exist')
        sys.exit()

def extract_stock_symbol_from_path(file_path, from_format, to_format):
    symbol_name = file_path.split('/')[-1].split('.')[0]
    return format_stock_symbol(symbol_name, from_format, to_format)

# def extract_stock_symbol_from_path(file_path, from_format, to_format):
#     symbol_name = file_path.split('/')[-1].split('.')[0]
#     if from_format == 'MARKETnumber':
#         if to_format == 'MARKETnumber':
#             return symbol_name
#         else:
#             print('The to_format input does not exist')
#             sys.exit()
#     elif from_format == 'number_MARKET':
#         symbol_name_splited = symbol_name.split('_')
#         if to_format == 'number.MARKET':
#             return symbol_name_splited[0] + '.' + symbol_name_splited[1]
#         elif to_format == 'MARKETnumber':
#             return symbol_name_splited[1] + symbol_name_splited[0]
#         else:
#             print('The to_format input does not exist')
#             sys.exit()
#
#     elif from_format =='MARKETnumber_xxx':
#         if to_format == 'MARKETnumber':
#             return symbol_name.split('_')[0]
#         else:
#             print('The to_format input does not exist')
#             sys.exit()
#     else:
#         print('The from_format input does not exist')
#         sys.exit()



from datetime import datetime


def get_today_date_string(format_string: str = '%Y-%m-%d') -> str:
    """
    Returns today's date as a string in the specified format.

    Args:
        format_string: A string representing the desired date format
                       (default is 'YYYY-MM-DD').

    Returns:
        The current date formatted as a string.
    """
    # 1. Get the current datetime object
    today = datetime.now()

    # 2. Convert the datetime object to a string using the format_string
    date_string = today.strftime(format_string)

    return date_string