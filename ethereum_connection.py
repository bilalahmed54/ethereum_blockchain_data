#!/usr/bin/python
import psycopg2
from web3 import Web3, HTTPProvider
import sys
import pprint
import binascii

web3 = Web3(HTTPProvider('https://mainnet.infura.io/v3/bc49f2a51e744bdba3b670bc7aa291fc'))
print(web3.isConnected())

print(web3.eth.blockNumber)

conn_string = "host='localhost' dbname='ethereum_blockchain' user='postgres' password='root'"
print ("Connecting to database\n	->%s" % (conn_string))
conn = psycopg2.connect(conn_string)	

def main():
	loop_start = 5980198
	loop_end   = 6500001
	while (loop_start < loop_end):
		try:
			block_info = (dict(web3.eth.getBlock(loop_start)))
			#transaction_info  
			#print(block_info)
			difficulty  =str(block_info['difficulty'])  
			values = [] 
			gaslimit = block_info['gasLimit'] 
			gasused = block_info['gasUsed'] 
			hash_block = block_info['hash'].hex()
			miner = block_info['miner'] 
			number_u = block_info['number'] 
			transactionsroot  = block_info['transactionsRoot'].hex()
			size_b = block_info['size'] 
			stateroot = block_info['stateRoot'].hex()
			timestamp_b = block_info['timestamp'] 
			totalDifficulty = str(block_info['totalDifficulty'])  
			
			for i in block_info['transactions']:
				try:
					transaction_info = web3.eth.getTransaction(i) 
					
					sm_arr = [] 
					hash_transaction = transaction_info['hash'].hex()

					nonce_transaction = transaction_info['nonce']
					tran_from = transaction_info['from'] 
					
					tran_to = transaction_info['to'] 

					value_t = web3.fromWei(transaction_info['value'], 'ether') 
					gasprice_t = web3.fromWei(transaction_info['gasPrice'], 'ether') 
					gas_t = transaction_info['gas']
					input_1 =  transaction_info['input']  

					if(hash_transaction != None and nonce_transaction != None and tran_from!= None and tran_to != None and value_t != None and gasprice_t != None and gas_t!= None):
						sm_arr.append(input_1)
						sm_arr.append(number_u) 
						sm_arr.append(tran_from) 
						sm_arr.append(gas_t) 
						sm_arr.append(float(gasprice_t)) 
						sm_arr.append(hash_transaction) 
						sm_arr.append(nonce_transaction) 
						sm_arr.append(tran_to)						  
						sm_arr.append(float(value_t)) 
						values.append(sm_arr)
 
				except Exception as ex2:
					print("missing transactions \n")
					print(ex2)
			try:
				
				print(values[0])			 
				# conn.cursor will return a cursor object, you can use this cursor to perform queries
				cursor = conn.cursor()

				if len(values) > 1:
					cursor.execute("INSERT INTO transactions VALUES"+str(tuple([tuple(i) for i in tuple(values)]))[1:-1]+";")
				if len(values) == 1:
					cursor.execute("INSERT INTO transactions VALUES"+str(tuple([tuple(i) for i in tuple(values)]))[1:-2]+";")

				print("transactions Done")

				print(str(difficulty))
				print(str(gaslimit))
				print(str(gasused))
				print(str(hash_block))
				print(str(miner))
				print(str(number_u))
				print(str(size_b))
				print(str(stateroot))
				print(str(timestamp_b))
				print(str(totalDifficulty))
				print(str(transactionsroot))
				print(str(timestamp_b))

				cursor.execute("INSERT INTO updated_blocks VALUES ("+"'"+ str(difficulty)+"',"+ str(gaslimit)+","+ str(gasused)+",'"+str(hash_block)+"','"+str(miner)+"',"+ str(number_u)+","+str(size_b)+",'"+str(stateroot)+"',"+str(timestamp_b)+",'"+str(totalDifficulty)+"','"+str(transactionsroot)+"',"+"to_timestamp("+str(timestamp_b)+")"+");")	
				conn.commit()	
				cursor.close()

				print("iteration.."+str(loop_start))
				loop_start = loop_start+1

			except Exception as ex:
				print("error occured while INSERTing data \n")
				print(len(values))
				print(ex)

#				print("INSERT INTO transaction_1_gather VALUES"+str(tuple([tuple(i) for i in tuple(values)]))[1:-1]+";")

#				print("\n*****************\n")
#				print(values)
#				print ("INSERT INTO updated_blocks_gather VALUES ("+"'"+ str(difficulty)+"',"+ str(gaslimit)+","+ str(gasused)+",'"+str(hash_block)+"','"+str(miner)+"',"+ str(number_u)+","+str(size_b)+",'"+str(stateroot)+"',"+str(timestamp_b)+",'"+str(totalDifficulty)+"','"+str(transactionsroot)+"',"+"to_timestamp("+str(timestamp_b)+")"+");")

				break

		except Exception as e:
			print ("error occured while fetching block data Using API")
			print (e)

	conn.close()	#closing connection

if __name__ == "__main__":
	main()
