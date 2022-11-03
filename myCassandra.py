from cmath import inf
import json
import random
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider


# Conexão Cassandra

cloud_config = {
    'secure_connect_bundle': '<</PATH/TO/>>secure-connect-cassandra.zip'
}
auth_provider = PlainTextAuthProvider('<<CLIENT ID>>', '<<CLIENT SECRET>>')
cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
session = cluster.connect()

session.execute("USE cassandra;")


#### Tabelas usuario, vendedor, produtos, compra ####

print("Generete all tables in project...")
session.execute(
    "CREATE TABLE IF NOT EXISTS usuario (email text PRIMARY KEY, nome text, cpf text, end text, fav text);")
print("User created...")
session.execute(
    "CREATE TABLE IF NOT EXISTS vendedor (email text PRIMARY KEY, nome text, cpf text, end text);")
print("Vendedor created...")
session.execute(
    "CREATE TABLE IF NOT EXISTS produtos (id text PRIMARY KEY, nome text, preco text, vendedor text);")
print("Produtos created...")
session.execute(
    "CREATE TABLE IF NOT EXISTS compra (id text PRIMARY KEY, usuario text, produto text, total text);")
print("Successful!")


#### Intert ####

# Usuario
def insertUser(nome, cpf, email, end, tel):
    preInsert = session.prepare(
        "INSERT INTO usuario (nome, cpf, email, end, tel) VALUES (?, ?, ?, ?);")

    session.execute(preInsert, [nome, cpf, email, end, tel])

# Vendedor


def insertVendedor(nome, cpf, email, end, tel):
    preInsert = session.prepare(
        "INSERT INTO vendedor (nome, email, end, tel) VALUES (?, ?, ?, ?);")

    session.execute(preInsert, [nome, email, end, tel])

# Produtos


def insertProduct(id, nome, preco, quantidade, vendedorEmail):
    vendedor = session.execute("SELECT * FROM vendedor;")

    for vend in vendedor:
        if (vend.email == vendedorEmail):
            vendInfo = [vendedorEmail, vendedor.nome]
            preInsert = session.prepare(
                "INSERT INTO produto (id, nome, preco, quantidade, vendedor) VALUES (?, ?, ?, ?, ?);")
        id = str(random.randint(1, 1000))
    session.execute(
        preInsert, [id, nome, preco, quantidade, vendInfo])


# Compras

def compras(usuario, produto, status, data, pagamento, quantidade, vendedorEmail, userEmail):

    produto = session.execute("SELECT * FROM produto;")
    vendedor = session.execute("SELECT * FROM vendedor;")
    users = session.execute("SELECT * FROM usuario;")

    for prod in produto:
        if (prod.nome == produto):
            for vend in vendedor:
                if (vend.email == vendedorEmail):
                    for user in users:
                        if (user.email == userEmail):
                            precoTotal = float(prod.preco) * int(quantidade)

                            preInsert = session.prepare(
                                "INSERT INTO compra (precototal, status, data, pagamento, produto, vendedor, usuario) VALUES (?, ?, ?, ?, ?, ?, ?);")

                            session.execute(preInsert, [str(precoTotal), usuario, status, data, pagamento, [
                                            prod.id, produto, prod.preco, quantidade], [vendedorEmail, vendedor.nome], [userEmail, user.nome]])

# Favoritos


def insertFav(nomeProd, email, preco, quantidade):

    produto = session.execute("SELECT * FROM produto;")
    users = session.execute("SELECT * FROM usuario;")

    for user in users:
        if (user.email == email):
            for prod in produto:
                if (prod.nome == nomeProd, preco, quantidade):
                    preInsert = session.prepare(
                        "UPDATE usuario SET fav = fav + ? WHERE email = ?;")

                    session.execute(preInsert, [
                                    [prod.id, nomeProd, preco, quantidade, prod.preco, prod.vendedor[0], prod.vendedor[1]], email])


#### Função Find ####

def findUsers():
    users = session.execute("SELECT * FROM usuario;")
    usersList = [{}]

    for user in users:
        usersList.append({user.email, user.nome, user.tel, user.cpf,
                         user.endereco[0], user.endereco[1], user.endereco[2], user.endereco[3]})
        print(usersList)


def findVendedores():
    vendedores = session.execute("SELECT * FROM vendedor;")
    vendedoresList = [{}]

    for vend in vendedores:
        vendedoresList.append({vend.email, vend.nome, vend.tel,
                              vend.endereco[0], vend.endereco[1], vend.endereco[2], vend.endereco[3]})
        print(vendedoresList)


def findProdutos():
    produtos = session.execute("SELECT * FROM produto;")
    produtosList = {}

    for prod in produtos:
        if (prod.id == id):
            produtosList.append({prod.nome, prod.preco, prod.quantidade,
                                prod.vendedor[0], prod.vendedor[1]})
            print(produtosList)


def findCompras():
    compras = session.execute("SELECT * FROM compra;")
    comprasList = [{}]

    for comp in compras:
        if (comp.id == id):
            comprasList.append({comp.id, comp.precototal, comp.status, comp.data, comp.formapagamento,
                               comp.produto[0], comp.produto[1], comp.produto[2], comp.produto[3], comp.vendedor[0], comp.vendedor[1], comp.usuario[0], comp.usuario[1]})

    print(comprasList)


def findUserFav(email):
    users = session.execute("SELECT * FROM usuario;")

    for user in users:
        if (user.email == email):
            favoritos = user.fav

            index = 0

            while (index < len(favoritos)):

                print(f'Id: {favoritos[index]}')
                print(f'Name: {favoritos[index + 1]}')
                print(f'Preco: {favoritos[index + 2]}')
                print(f'Vendedor Email: {favoritos[index + 3]}')
                print(f'Vendedor Name: {favoritos[index + 4]}')
                index += 5


#### Update ####


#### Delete ####
