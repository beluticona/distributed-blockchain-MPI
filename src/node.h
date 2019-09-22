#ifndef NODE_H
#define NODE_H

#include <mpi.h>
#include "block.h"

/* Tema del mensaje */
#define TAG_NEW_BLOCK 10
#define TAG_CHAIN_HASH 21
#define TAG_CHAIN_RESPONSE 22

/* Cantidad máxima de bloques de la blockchain */
#define MAX_BLOCKS 25

extern MPI_Datatype* MPI_BLOCK;

void broadcast_block(const Block& block);
void migrar_cadena(const Block blockchain[], unsigned int block_count,
    bool blockchains_share_blocks);

void* proof_of_work(void *ptr);
int node();
void verificar_y_migrar_cadena(const Block& received_block);

bool check_coherence_for_chain(const Block blockchain[], unsigned int block_count,
    bool& blockchains_share_blocks);

#endif  // NODE_H
