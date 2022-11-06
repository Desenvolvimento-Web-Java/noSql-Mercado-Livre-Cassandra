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
print("Gerar todas as tabelas")
session.execute(
    "CREATE TABLE IF NOT EXISTS usuario (email text PRIMARY KEY, nome text, cpf text, tel text, end text, fav text);")
print("Criação do usuário")
session.execute(
    "CREATE TABLE IF NOT EXISTS vendedor (email text PRIMARY KEY, nome text, cpf text, tel text, end text);")
print("Criação do vendedor")
session.execute(
    "CREATE TABLE IF NOT EXISTS produtos (id text PRIMARY KEY, nome text, preco text, quant text, vendedor text);")
print("Criação de produtos")
session.execute(
    "CREATE TABLE IF NOT EXISTS compra (id text PRIMARY KEY, usuario text, produto text, total text, pagamento text, quant text, vend text, user text);")
print("Criado com sucesso")

#### Intert ####

# Usuario
def insertUser(nome, cpf, email, end, tel):
    preInsert = session.prepare(
        "INSERT INTO usuario (nome, cpf, email, end, tel) VALUES (?, ?, ?, ?, ?);")

    session.execute(preInsert, [nome, cpf, email, end, tel])



# Vendedor
def insertVendedor(nome, cpf, email, end, tel):
    preInsert = session.prepare(
        "INSERT INTO vendedor (nome, cpf, email, end, tel) VALUES (?, ?, ?, ?, ?);")

    session.execute(preInsert, [nome, cpf, email, end, tel])



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
def insertCompras(id, usuario, produto, data, pagamento, quantidade, vendedorEmail, userEmail):

    produtos = session.execute("SELECT * FROM produto;")
    vendedor = session.execute("SELECT * FROM vendedor;")
    users = session.execute("SELECT * FROM usuario;")

    for prod in produtos:
        if (prod.nome == produtos):
            for vend in vendedor:
                if (vend.email == vendedorEmail):
                    for user in users:
                        if (user.email == userEmail):
                            valorTotal = float(prod.preco) * int(quantidade)

                            compraInsert = session.apresenta(
                                "INSERT INTO compra (id, valorTotal, data, pagamento, produto, vendedor, usuario) VALUES (?, ?, ?, ?, ?, ?, ?);")

                            session.execute(compraInsert, [id, str(valorTotal), usuario, data, pagamento, [
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


#### Find ####
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


def findFavoritos(email):
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
def updateUser(email, nome, tel, endereco):
    users = session.execute("SELECT * FROM usuario;")

    for user in users:
        if (user.email == email):
            session.execute("UPDATE usuario SET nome = '%s', email = '%s', tel = '%s', endereco = ['%s', '%s', '%s', '%s',] WHERE email = '%s'" % (nome, tel, endereco[0], endereco[1], endereco[2], endereco[3], email))
            
            findUsers(email)


def updateVendedor(email, tel, nome, endereco):
    vendedores = session.execute("SELECT * FROM vendedor;")

    for vend in vendedores:
        if (vend.email == email):
            session.execute("UPDATE vendedor SET nome = '%s', tel = '%s', endereco = ['%s', '%s', '%s', '%s'] WHERE email = '%s'" % (nome, tel, endereco[0], endereco[1], endereco[2], endereco[3], email))
            
            findVendedores(email)


def updateProduto(id, nome, preco, quantidade, vendedorEmail):
    produtos = session.execute("SELECT * FROM produto;")

    vendedor = session.execute("SELECT * FROM vendedor;")

    for vend in vendedor:
        if (vend.email == vendedorEmail):
            for prod in produtos:
                if (prod.id == id):
                    session.execute("UPDATE produto SET nome = '%s', preco = '%s', quantidade = '%s', vendedor = ['%s', '%s'] WHERE id = '%s'" % (nome, preco, quantidade, vendedorEmail, vendedor.nome, id))
                    
                    findProdutos(id)


#### Delete ####
def deleteUser(email):
    users = session.execute("SELECT * FROM usuario;")

    for user in users:
        if (user.email == email):
            session.execute("DELETE FROM usuario WHERE email = '%s'" % email)
            
            findUsers()


def deleteVendedor(email):
    vendedores = session.execute("SELECT * FROM vendedor;")

    for vend in vendedores:
        if (vend.email == email):
            session.execute("DELETE FROM vendedor WHERE email = '%s'" % email)
            
            findVendedores()


def deleteProduto(id):
    produtos = session.execute("SELECT * FROM produto;")

    for prod in produtos:
        if (prod.id == id):
            session.execute("DELETE FROM produto WHERE id = '%s'" % id)
            
            findProdutos()


