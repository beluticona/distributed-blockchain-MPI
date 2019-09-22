#include "node.h"
#include "picosha2.h"
#include <stdio.h>
#include <string.h>
#include <pthread.h>
#include <cstdlib>
#include <queue>
#include <atomic>
#include <mpi.h>
#include <map>

#include <mutex>

#define INIT_NULL_BLOCK 0

int total_nodes, my_mpi_rank;
pthread_t thread_minador;
mutex mtx;

//Por cada nuevo bloque y por cada re estructuración, actualizar último de la cadena
Block last_block_in_chain;

/*
  * Actualizar al:
      - Agregar bloque
      - Recibir notificación de bloque
      - Reestructurar cadena (eliminar bloques del diccionario)
*/
map<string,Block> node_blocks;

//Cuando me llega una cadena adelantada, y tengo que pedir los nodos que me faltan
//Si nos separan más de VALIDATION_BLOCKS bloques de distancia entre las cadenas, se descarta por seguridad
void verificar_y_migrar_cadena(const Block& received_block){
  //¡¡SOMOS ALICE!! - ¡¡BOB!! ¡¡RESPONDEME!!

  // @TODO Ver qué pasa si esta cadena está atrasada una cantidad de bloques > a VALIDATION_BLOCKS

  // Pido a Bob la cadena completa
  unsigned int received_block_owner = received_block.node_owner_number;
  MPI_Send(received_block.block_hash, HASH_SIZE, MPI_CHAR, received_block_owner, TAG_CHAIN_HASH,
    MPI_COMM_WORLD);
  //printf("[%d] Envié una solicitud de cadena a %d \n",my_mpi_rank, received_block_owner);

  // Me bloqueo esperando la respuesta de Bob
  MPI_Status status;
  Block blockchain[VALIDATION_BLOCKS];
  MPI_Recv(&blockchain, VALIDATION_BLOCKS, *MPI_BLOCK, received_block_owner, TAG_CHAIN_RESPONSE,
    MPI_COMM_WORLD, &status);
  printf("[%d] RECV BLOCKCHAIN FROM [%d] \n",my_mpi_rank, received_block_owner);

  // Verifico que el último bloque de la cadena recibida sea el que yo pedí
  Block& last_block = blockchain[0];
  if (received_block.index != last_block.index or
      string(received_block.block_hash) != last_block.block_hash) {
    printf("[%d] DISCARD BLOCKCHAIN: LAST BLOCK DIFFERS FROM RECEIVED (ANOTHER FORK)\n",my_mpi_rank);
    return;
  }

  // Averiguo el tamaño de la cadena recibida
  int block_count; 
  MPI_Get_count(&status, *MPI_BLOCK, &block_count);

  // Verifico la coherencia de la cadena recibida
  bool blockchains_share_blocks;
  if (not check_coherence_for_chain(blockchain, block_count, blockchains_share_blocks)) return;

  migrar_cadena(blockchain, block_count, blockchains_share_blocks);
}

void migrar_cadena(const Block blockchain[], unsigned int block_count,
    bool blockchains_share_blocks) {
  const Block& first_block = blockchain[block_count - 1];

  if (not blockchains_share_blocks and first_block.index > 1) {
    printf("[%d] DISCARD BLOCKCHAIN: UNSAFE\n",my_mpi_rank);
    return;
  }

  //Va agregando los bloques de la cadena recibida hasta encontrar el primero que ya tenga
  string hash;
  for (unsigned int i = 0; i < block_count; ++i){
    hash = blockchain[i].block_hash;
    if (node_blocks.count(hash) == 1) break;
    node_blocks[hash] = blockchain[i];
  }

  unsigned int index = blockchains_share_blocks ? node_blocks[hash].index : 0;

  //Delete de mis bloques que no forman parte de la nueva blockchain (los que tenia solo yo)
  Block& block_to_clean = last_block_in_chain;
  while (block_to_clean.index > index) {
    node_blocks.erase(string(block_to_clean.block_hash));
    block_to_clean = node_blocks[string(block_to_clean.previous_block_hash)];
  }

  last_block_in_chain = blockchain[0];
   printf("[%d] MIGRATE\n",my_mpi_rank);
}

//Si falla alguna condición diremos que el bloque es corrupto, por lo tanto
//se descarta la migración
bool check_coherence_for_chain(const Block blockchain[], unsigned int block_count,
    bool& blockchains_share_blocks) {
  blockchains_share_blocks = false;
  string calculated_hash;
  //Recorro la cadena desde el último bloque 
  for (unsigned int i = 0; i < block_count - 1; ++i) {
    const Block& actual = blockchain[i];
    const Block& prev = blockchain[i + 1];

    //Chequear que coinciden los hashes de bloques contiguos
    if (string(actual.previous_block_hash) != prev.block_hash) {
      printf("[%d] DISCARD BLOCKCHAIN: UNCONNECTED HASHES IN BLOCKS %d AND %d\n",my_mpi_rank,
        prev.index, actual.index);
      return false;
    }
    
    //Chequear que coinciden los índices de bloques contiguos
    if (actual.index != prev.index +1) {
      printf("[%d] DISCARD BLOCKCHAIN: UNCONNECTED INDICES IN BLOCKS %d AND %d\n",my_mpi_rank,
        prev.index, actual.index);
      return false;
    }

    // Verifico que el hash indicado en el bloque coincida con el calculado                                  
    block_to_hash(&actual, calculated_hash);
    if (actual.block_hash != calculated_hash) {
      printf("[%d] DISCARD BLOCKCHAIN: INVALID HASH IN BLOCK %d\n",my_mpi_rank, actual.index);
      return false;
    }

    if (node_blocks.count(calculated_hash) == 1) blockchains_share_blocks = true;
  }

  // Hago los chequeos al primer bloque porque queda afuera del ciclo for
  const Block& first_block = blockchain[block_count - 1];
  block_to_hash(&first_block, calculated_hash);
  if (first_block.block_hash != calculated_hash) {
    printf("[%d] DISCARD BLOCKCHAIN: INVALID HASH IN BLOCK %d\n",my_mpi_rank, first_block.index);
    return false;
  }

  if (node_blocks.count(calculated_hash) == 1) blockchains_share_blocks = true;

  //printf("[%d] se cumplen todas las condiciones para la migracion de cadena \n",my_mpi_rank);
  return true;
}

// Envia el bloque minado a todos los nodos
void broadcast_block(const Block& block) {
  // No enviar a mí mismo
  // Si más adelante queremos que sea no bloqueante, descomentar Isend y la declaración de request
  // MPI_Request request[total_nodes];
  for (int i = (my_mpi_rank + 1) % total_nodes; i != my_mpi_rank; i = (i + 1) % total_nodes) {
    MPI_Send(&block, 1, *MPI_BLOCK, i, TAG_NEW_BLOCK, MPI_COMM_WORLD);
    // MPI_Isend(block, 1, *MPI_BLOCK, i, TAG_NEW_BLOCK, MPI_MPI_COMM_WORLD, &request[i]);
    //printf("[%d] Envié mi bloque minado a %d \n",my_mpi_rank,i);
  }
}

//Proof of work
//TODO: Advertencia: puede tener condiciones de carrera
void* proof_of_work(void *ptr){
    string hash_hex_str;
    unsigned int mined_blocks = 0;
    while(true){
      mtx.lock();
      Block block = last_block_in_chain;
      mtx.unlock();

      //Preparar nuevo último bloque
      block.index += 1;
      block.node_owner_number = my_mpi_rank;
      block.difficulty = DEFAULT_DIFFICULTY;
      block.created_at = static_cast<unsigned long int> (time(NULL));
      /* Seteo el previos block hash como el viejo las_block_in_chain  */
      memcpy(block.previous_block_hash,block.block_hash,HASH_SIZE);

      //Agregar un nonce al azar al bloque para intentar resolver el problema
      gen_random_nonce(block.nonce);

      //Hashear el contenido (con el nuevo nonce) Cde_blocks[hash_hex_str] = *lrea el hash del nuevo bloque
      block_to_hash(&block,hash_hex_str);

      //Contar la cantidad de ceros iniciales (con edde_blocks[hash_hex_str] = *le_blocde_blocks[hash_hex_str] = *lde_blocks[hash_hex_str] = *lde_blocks[hash_hex_str] = *lks[hash_hex_str] = *ll nuevo nonce)
      if(solves_problem(hash_hex_str)){

          //Verifico que no haya cambiado mientras calculaba
          //Sección crítica, no quiero que el otro thread_receptor modifique el last_block_in_chain
          // como el node_block map
          mtx.lock();
          //printf("[%d] Pido mutex para minar el bloque %d \n",my_mpi_rank,block.index);

          if(last_block_in_chain.index < block.index){
            mined_blocks += 1;
            last_block_in_chain = block;
            //Actualiza el hash del bloque que ahora es el último
            strcpy(last_block_in_chain.block_hash, hash_hex_str.c_str());
            //Actualizo el diccionario
            node_blocks[hash_hex_str] = last_block_in_chain;
            // Aún no libero el mutex porque tengo que comunicar que este es el nuevo último bloque
            printf("----------------------------------------\n[%d] ADD BLOCK %d\n",my_mpi_rank,last_block_in_chain.index);

            if(last_block_in_chain.index == MAX_BLOCKS){
              printf("[%d] WIN\n",my_mpi_rank);
              MPI_Abort(MPI_COMM_WORLD, 0);
            }
            broadcast_block(last_block_in_chain);
            //Libero el mutex para que el receptor continúe con la conveción (puede modificar el last_block_in_chain)
          }
          mtx.unlock();
      }

    }

    return NULL;
}

int node(){

  //Tomar valor de my_mpi_rank y de nodos totales
  MPI_Comm_size(MPI_COMM_WORLD, &total_nodes);
  MPI_Comm_rank(MPI_COMM_WORLD, &my_mpi_rank);

  //La semilla de las funciones aleatorias depende del my_mpi_ranking
  srand(time(NULL) + my_mpi_rank);
  printf("[MPI] LAUNCHING PROCESS %u\n", my_mpi_rank);

  //Inicializo el primer bloque
  last_block_in_chain.index = INIT_NULL_BLOCK;
  last_block_in_chain.node_owner_number = my_mpi_rank;
  last_block_in_chain.difficulty = DEFAULT_DIFFICULTY;
  last_block_in_chain.created_at = static_cast<unsigned long int> (time(NULL));

  /*Escribe tantos ceros como el tamaño de las HASH_SIZE en el puntero al anteúltimo bloque*/
  memset(last_block_in_chain.previous_block_hash,0,HASH_SIZE);

  pthread_create(&thread_minador, NULL, proof_of_work, NULL); 

  while(true){
    MPI_Status status;
    MPI_Probe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);

    switch (status.MPI_TAG) {
      case TAG_NEW_BLOCK:
        //Recibo bloque y lo guardo en receive_block
        Block received_block;
        MPI_Recv(&received_block, 1, *MPI_BLOCK, MPI_ANY_SOURCE, TAG_NEW_BLOCK, MPI_COMM_WORLD, &status);
        //printf("[%d] Recibi un bloque de %d \n",my_mpi_rank, status.MPI_SOURCE);

        mtx.lock();
        if(valid_new_block(&received_block)){
          const unsigned int index_received = received_block.index;
          const unsigned int my_last_index = last_block_in_chain.index;

          if(index_received == 1 && my_last_index == INIT_NULL_BLOCK){
            //Si el índice del bloque recibido es 1 y mí último bloque actual tiene índice 0,
            //entonces lo agrego como nuevo último.
            node_blocks[string(received_block.block_hash)] = received_block;
            last_block_in_chain = received_block; 
            printf("[%d] ADD BLOCK %d FROM [%d]\n", my_mpi_rank, index_received,status.MPI_SOURCE);
          } else if(index_received == my_last_index + 1 && 
              string(received_block.previous_block_hash) == last_block_in_chain.block_hash){
            //Si el índice del bloque recibido es el siguiente a mí último bloque actual,
            //y el bloque anterior apuntado por el recibido es mí último actual, entonces 
            //lo agrego como nuevo último.
            node_blocks[string(received_block.block_hash)] = received_block;
            last_block_in_chain = received_block;
            printf("[%d] ADD BLOCK %d FROM [%d]\n", my_mpi_rank, index_received,status.MPI_SOURCE);         
          } else if(index_received == my_last_index + 1 && 
              string(received_block.previous_block_hash) != last_block_in_chain.block_hash){
            //Si el índice del bloque recibido es el siguiente a mí último bloque actual,
            //pero el bloque anterior apuntado por el recibido no es mí último actual,
            //entonces hay una blockchain más larga que la mía.
            printf("[%d] REQ BLOCKCHAIN TO [%d] UP TO BLOCK %d\n", my_mpi_rank, status.MPI_SOURCE, index_received);
            verificar_y_migrar_cadena(received_block);
          } else if(my_last_index + 1 < index_received){
            //Si el índice del bloque recibido está más de una posición adelantada a mi último
            //bloque actual, entonces me conviene abandonar mi blockchain actual
            printf("[%d] REQ BLOCKCHAIN TO [%d] UP TO BLOCK %d\n", my_mpi_rank, status.MPI_SOURCE, index_received);
            verificar_y_migrar_cadena(received_block);
          } else if(index_received < last_block_in_chain.index) {
            //Si el índice del bloque recibido es anterior al índice de mi último bloque actual,
            //entonces lo descarto porque asumo que mi cadena es la que está quedando preservada.
            printf("[%d] DISCARD BLOCK %d FROM [%d]\n",my_mpi_rank,index_received,status.MPI_SOURCE);
          } else if(index_received == last_block_in_chain.index) {
            //Si el índice del bloque recibido es igua al índice de mi último bloque actual,
            //entonces hay dos posibles forks de la blockchain pero mantengo la mía
            printf("[%d] DISCARD BLOCK %d FROM [%d]\n",my_mpi_rank,index_received,status.MPI_SOURCE);
          }
        } else {
          printf("[%d] INVALID BLOCK %d FROM [%d]\n", my_mpi_rank, received_block.index,status.MPI_SOURCE);
        }
        mtx.unlock();
        break;
      case TAG_CHAIN_HASH:
        //Si es un mensaje de pedido de cadena, responderlo enviando los bloques correspondientes
        char hash[HASH_SIZE];
        //printf("[%d] Recibo pedido de enviar blockchain a %d \n",my_mpi_rank, status.MPI_SOURCE);
        MPI_Recv(&hash, HASH_SIZE, MPI_CHAR, MPI_ANY_SOURCE, TAG_CHAIN_HASH, MPI_COMM_WORLD, &status);
        //printf("[%d] Recibi un pedido de enviar blockchain a %d \n",my_mpi_rank, status.MPI_SOURCE);
        Block& block = node_blocks[string(hash)];
        const unsigned int chain_size = block.index;


        //Armo la cadena para enviar con tantos bloques como validation blocks
        const unsigned int to_send_size = (chain_size > VALIDATION_BLOCKS)? VALIDATION_BLOCKS : chain_size;
        Block blocks[to_send_size];

        for (unsigned int i = 0; i < to_send_size; ++i) {
          blocks[i] = block;
          block = node_blocks[string(block.previous_block_hash)]; 
        }

        MPI_Send(&blocks, to_send_size, *MPI_BLOCK, status.MPI_SOURCE, TAG_CHAIN_RESPONSE, MPI_COMM_WORLD);
        printf("[%d] SEND BLOCKCHAIN TO [%d]\n",my_mpi_rank, status.MPI_SOURCE);
        break;
    }
  }
  return 0;
}
