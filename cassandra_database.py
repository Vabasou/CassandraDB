from cassandra.cluster import Cluster

KEYSPACE = "Phones"
cluster = Cluster(['localhost'], port = 9042)
session = cluster.connect()

rows = session.execute("SELECT keyspace_name FROM system_schema.keyspaces")
if KEYSPACE in [row[0] for row in rows]:
    print("Keyspace by the name \"" + KEYSPACE + "\" already exists, dropping.")
    session.execute("DROP KEYSPACE " + KEYSPACE)

print ("Creating keyspace \"" + KEYSPACE + "\".")
session.execute("""
    CREATE KEYSPACE %s
    WITH REPLICATION =
    { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }
""" % KEYSPACE)

print("Setting keyspace \"" + KEYSPACE + "\":")
session.execute('USE %s' % KEYSPACE)

# clients
session.execute("""
                CREATE TABLE clients (
                    client_id INT,
                    client_name VARCHAR,
                    phone_id INT,
                    address VARCHAR,
                    city VARCHAR,
                    PRIMARY KEY (client_id)
                )
                """)

# phones
session.execute("""
                CREATE TABLE phones (
                    client_id INT,
                    phone_id INT,
                    PRIMARY KEY (phone_id)
                )
                """)

# components
session.execute("""
                CREATE TABLE components (
                    component_type VARCHAR,  
                    component_price FLOAT,
                    PRIMARY KEY (component_type)
                )
                """)

# phones_by_client
session.execute(""" 
                CREATE TABLE phones_by_client (
                    phone_id INT,
                    client_id INT,
                    client_name VARCHAR,
                    phone_model VARCHAR,
                    desc_of_repair VARCHAR,
                    PRIMARY KEY ((client_id), phone_id, client_name)
                ) 
                """)

# phones_by_model
session.execute(""" 
                CREATE TABLE phones_by_model (
                    phone_id INT,
                    client_id INT,
                    client_name VARCHAR,
                    phone_model VARCHAR,
                    desc_of_repair VARCHAR,
                    PRIMARY KEY ((phone_model), phone_id)
                ) 
                """)

# phones_by_compnents
session.execute("""
                CREATE TABLE phones_by_components (
                    phone_id INT,
                    client_id INT,
                    client_name VARCHAR,
                    component_type VARCHAR,
                    component_price FLOAT,
                    PRIMARY KEY ((component_type), phone_id)
                )
                """)

# Insert in clients Table
session.execute("INSERT INTO clients (client_id, client_name, phone_id, address, city) VALUES (1, 'Vytautas Beiga', 1, 'Address:1', 'Vilnius') IF NOT EXISTS")
session.execute("INSERT INTO clients (client_id, client_name, phone_id, address, city) VALUES (2, 'Ignas Bieksa', 2, 'Address:2', 'Vilnius') IF NOT EXISTS")
session.execute("INSERT INTO clients (client_id, client_name, phone_id, address, city) VALUES (3, 'Algirdas Beiga', 3, 'Address:3', 'Kaunas') IF NOT EXISTS")
session.execute("INSERT INTO clients (client_id, client_name, phone_id, address, city) VALUES (4, 'Vytautas Beiga', 1, 'Address:1', 'Vilnius') IF NOT EXISTS") # Different client id
session.execute("INSERT INTO clients (client_id, client_name, phone_id, address, city) VALUES (2, 'Ignas Bieksa', 2, 'Address:2', 'Vilnius') IF NOT EXISTS") # Insert the same

# Insert in phones Table
session.execute("INSERT INTO phones (client_id, phone_id) VALUES (1, 1) IF NOT EXISTS")
session.execute("INSERT INTO phones (client_id, phone_id) VALUES (2, 2) IF NOT EXISTS")
session.execute("INSERT INTO phones (client_id, phone_id) VALUES (3, 3) IF NOT EXISTS")
session.execute("INSERT INTO phones (client_id, phone_id) VALUES (2, 2) IF NOT EXISTS")
session.execute("INSERT INTO phones (client_id, phone_id) VALUES (1, 4) IF NOT EXISTS")

# Insert in components table
session.execute("INSERT INTO components (component_type, component_price) VALUES ('Samsung S9 RAM', 50) IF NOT EXISTS")
session.execute("INSERT INTO components (component_type, component_price) VALUES ('Samsung S9 Screen', 100) IF NOT EXISTS")
session.execute("INSERT INTO components (component_type, component_price) VALUES ('Iphone 13 CPU', 150) IF NOT EXISTS")
session.execute("INSERT INTO components (component_type, component_price) VALUES ('Iphone 13 RAM', 55) IF NOT EXISTS")
session.execute("INSERT INTO components (component_type, component_price) VALUES ('Nokia Screen', 200) IF NOT EXISTS")
session.execute("INSERT INTO components (component_type, component_price) VALUES ('Samsung S9 SSD', 100) IF NOT EXISTS")

# Insert in phones_by_client table
session.execute("INSERT INTO phones_by_client (phone_id, client_id, client_name, phone_model, desc_of_repair) VALUES (1, 1, 'Vytautas Beiga', 'Samsung S9', 'Very old') IF NOT EXISTS")
session.execute("INSERT INTO phones_by_client (phone_id, client_id, client_name, phone_model, desc_of_repair) VALUES (2, 2, 'Ignas Bieksa', 'Samsung S10', 'Old') IF NOT EXISTS")
session.execute("INSERT INTO phones_by_client (phone_id, client_id, client_name, phone_model, desc_of_repair) VALUES (3, 3, 'Algirdas Beiga', 'Iphone 13', 'New') IF NOT EXISTS")
session.execute("INSERT INTO phones_by_client (phone_id, client_id, client_name, phone_model, desc_of_repair) VALUES (4, 1, 'Vytautas Beiga', 'Nokia 3310', 'Legendary') IF NOT EXISTS")
         
# Insert in phones_by_model table
session.execute("INSERT INTO phones_by_model (phone_id, client_id, client_name, phone_model, desc_of_repair) VALUES (1, 1, 'Vytautas Beiga', 'Samsung S9', 'Very old') IF NOT EXISTS")
session.execute("INSERT INTO phones_by_model (phone_id, client_id, client_name, phone_model, desc_of_repair) VALUES (2, 2, 'Ignas Bieksa', 'Samsung S9', 'Old') IF NOT EXISTS")
session.execute("INSERT INTO phones_by_model (phone_id, client_id, client_name, phone_model, desc_of_repair) VALUES (3, 3, 'Algirdas Beiga', 'Iphone 13', 'New') IF NOT EXISTS")
session.execute("INSERT INTO phones_by_model (phone_id, client_id, client_name, phone_model, desc_of_repair) VALUES (4, 1, 'Vytautas Beiga', 'Nokia 3310', 'Legendary') IF NOT EXISTS")

# Insert in phones_by_component table
session.execute("INSERT INTO phones_by_components (phone_id, client_id, client_name, component_type, component_price) VALUES (1, 1, 'Vytautas Beiga', 'RAM', 150) IF NOT EXISTS")
session.execute("INSERT INTO phones_by_components (phone_id, client_id, client_name, component_type, component_price) VALUES (2, 2, 'Ignas Bieksa', 'Screen', 50) IF NOT EXISTS")
session.execute("INSERT INTO phones_by_components (phone_id, client_id, client_name, component_type, component_price) VALUES (3, 3, 'Algirdas Beiga', 'CPU', 100) IF NOT EXISTS")
session.execute("INSERT INTO phones_by_components (phone_id, client_id, client_name, component_type, component_price) VALUES (4, 1, 'Vytautas Beiga', 'None', 5) IF NOT EXISTS")

# Queries

# clients
print("Query's << SELECT * FROM clients >> results:")
print("__________________________________________________________")
rows = session.execute('SELECT * FROM clients')
for row in rows:
    print("client_id: ", row.client_id, " | client_name: ", row.client_name, " | phone_id: ", row.phone_id, " | address: ", row.address, " | city: ", row.city)
print()

# phones
print("Query's << SELECT * FROM phones >> results:")
print("__________________________________________________________")
rows = session.execute('SELECT * FROM phones')
for row in rows:
    print("client_id: ", row.client_id, " | phone_id: ", row.phone_id)
print()

# components
print("Query's << SELECT * FROM components >> results:")
print("__________________________________________________________")
rows = session.execute('SELECT * FROM components')
for row in rows:
    print("component_type: ", row.component_type, " | component_price: ", row.component_price)
print()

# phones_by_client
print("Query's << SELECT * FROM phones_by_client WHERE client_id = 1>> results:")
print("__________________________________________________________")
rows = session.execute('SELECT * FROM phones_by_client WHERE client_id = 1')
for row in rows:
    print("phone_id: ", row.phone_id, " | client_id: ", row.client_id, " | client_name: ", row.client_name, " | phone_model: ", row.phone_model, " | desc_of_repair: ", row.desc_of_repair)
print()

print("Query's << SELECT * FROM phones_by_client WHERE client_id = 1 AND phone_id = 1>> results:")
print("__________________________________________________________")
rows = session.execute('SELECT * FROM phones_by_client WHERE client_id = 1 AND phone_id = 1')
for row in rows:
    print("phone_id: ", row.phone_id, " | client_id: ", row.client_id, " | client_name: ", row.client_name, " | phone_model: ", row.phone_model, " | desc_of_repair: ", row.desc_of_repair)
print()

# phones_by_model
print("Query's << SELECT * FROM phones_by_model WHERE phone_model = 'Samsung S9'>> results:")
print("__________________________________________________________")
rows = session.execute("SELECT * FROM phones_by_model WHERE phone_model = 'Samsung S9'")
for row in rows:
    print("phone_id: ", row.phone_id, " | client_id: ", row.client_id, " | client_name: ", row.client_name, " | phone_model: ", row.phone_model, " | desc_of_repair: ", row.desc_of_repair)
print()

print("Query's << SELECT * FROM phones_by_model WHERE phone_model = 'Samsung S9' AND phone_id = 1>> results:")
print("__________________________________________________________")
rows = session.execute("SELECT * FROM phones_by_model WHERE phone_model = 'Samsung S9' AND phone_id = 1")
for row in rows:
    print("phone_id: ", row.phone_id, " | client_id: ", row.client_id, " | client_name: ", row.client_name, " | phone_model: ", row.phone_model, " | desc_of_repair: ", row.desc_of_repair)
print()

# phones_by_compnents
print("Query's << SELECT * FROM phones_by_components WHERE component_type = 'RAM'>> results:")
print("__________________________________________________________")
rows = session.execute("SELECT * FROM phones_by_components WHERE component_type = 'RAM'")
for row in rows:
    print("phone_id: ", row.phone_id, " | client_id: ", row.client_id, " | client_name: ", row.client_name, " | component_type: ", row.component_type, " | component_price: ", row.component_price)
print()

print("Query's << SELECT * FROM phones_by_components WHERE component_type = 'RAM' AND phone_id = 1>> results:")
print("__________________________________________________________")
rows = session.execute("SELECT * FROM phones_by_components WHERE component_type = 'RAM' AND phone_id = 1")
for row in rows:
    print("phone_id: ", row.phone_id, " | client_id: ", row.client_id, " | client_name: ", row.client_name, " | component_type: ", row.component_type, " | component_price: ", row.component_price)
print()

