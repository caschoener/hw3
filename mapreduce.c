/*
 * Implementation file for simple MapReduce framework.  Fill in the functions
 * and definitions in this file to complete the assignment.
 *
 * Place all of your implementation code in this file.  You are encouraged to
 * create helper functions wherever it is necessary or will make your code
 * clearer.  For these functions, you should follow the practice of declaring
 * them "static" and not including them in the header file (which should only be
 * used for the *public-facing* API.
 */


/* Header includes */
#include <stdlib.h>

#include "mapreduce.h"


/* Size of shared memory buffers */
#define MR_BUFFER_SIZE 1024

typedef struct buffer_node
{
	struct kvpair kv;
	struct buffer_node* next;
}Node;




/* Allocates and initializes an instance of the MapReduce framework */
struct map_reduce *
mr_create(map_fn map, reduce_fn reduce, int threads)
{
	struct map_reduce * mr = malloc(sizeof(struct map_reduce));
	
	int * n = malloc(sizeof(int)); //malloc space for int n
	*n = threads;
	
	int * m = calloc(sizeof(int) * threads); //allocate and initialize all elements to 0

	mr -> mapfn = map; //no need to malloc since map is a ptr to code
	mr -> reducefn = reduce; //same as above
	mr -> nmaps = n; //nmaps is pointer to int
	mr-> mr_buffer = malloc(sizeof(Node)* threads * 2); //mr_buffer will be an array of heads and tails, head(n) is at mr_buffer(2n) and tail at (2n+1) 
	mr -> buffer_count = m;
	mr -> p_array = malloc(sizeof(pthread_t)*threads);

//replace with linked list from here down
//counter to limit size, read from front write to back
//pointer to tail updates every write
//delete head after read and update pointer
//when list is empty, write to node and THEN update map_reduce->head pointer
//consume checks if head pointer is null
//no need for locks ever!

	
	
	
	
	for(int i = 0; i < (2*threads) ;i++) //need to initialize all heads and tails to NULL 
	{
		(mr-> mr_buffer)[i] = NULL;
	}
	
	return mr;
}

/* Destroys and cleans up an existing instance of the MapReduce framework */
void
mr_destroy(struct map_reduce *mr)
{		

	for(int i = 0; i < *(mr->nmaps) ;i++) //free linked list
	{
		free((mr-> mr_buffer)[i]);
	}	
	
	free(mr-> nmaps);
	free(mr-> mr_buffer);
	free(mr-> buffer_count);
	free(mr-> p_array);

	free(mr); //free the structure ptr
}

/* Begins a multithreaded MapReduce operation */
int
mr_start(struct map_reduce *mr, const char *inpath, const char *outpath)
{

	return 0;
}

/* Blocks until the entire MapReduce operation is complete */
int
mr_finish(struct map_reduce *mr)
{
	return 0;
}

/* Called by the Map function each time it produces a key-value pair */
int
mr_produce(struct map_reduce *mr, int id, const struct kvpair *kv)
{
	
	return 0;
}

/* Called by the Reduce function to consume a key-value pair */
int
mr_consume(struct map_reduce *mr, int id, struct kvpair *kv)
{
	return 0;
}
