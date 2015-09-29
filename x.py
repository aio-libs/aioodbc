import pyodbc
print('x' * 100)
print(pyodbc.dataSources())
print(pyodbc.connect('Driver={postgresql unicode}'))
