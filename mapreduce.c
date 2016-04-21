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
#include <string.h>
#include <stdio.h>
#include <fcntl.h>
#include <unistd.h>
#include "mapreduce.h"


/* Size of shared memory buffers */
#define MR_BUFFER_SIZE 1024

void map_helper(void*);
void reduce_helper(void*);




/* Allocates and initializes an instance of the MapReduce framework */
struct map_reduce *
mr_create(map_fn map, reduce_fn reduce, int threads)
{
	struct map_reduce * mr = malloc(sizeof(struct map_reduce));
	if (mr ==NULL) //handle out of memory situation (I guess)
	{
		 return NULL;
	}
	int * n = malloc(sizeof(int)); //malloc space for int n
	*n = threads;
	
	int * m = calloc(threads, sizeof(int)); //allocate and initialize all elements to 0

	mr -> mapfn = map; //no need to malloc since map is a ptr to code
	mr -> reducefn = reduce; //same as above
	mr -> nmaps = n; //nmaps is pointer to int
	mr -> mr_heads = malloc(sizeof(Node)*threads); 
	mr -> mr_tails = malloc(sizeof(Node)*threads); 
	mr -> buffer_count = m;
	mr -> p_array = malloc(sizeof(pthread_t)*(threads+1));
	mr -> lock_array = malloc(sizeof(pthread_mutex_t)*threads); //one lock per thread for accessing count
	mr -> count_lock = malloc(sizeof(pthread_mutex_t)*threads); //for maximum thread safety
	mr -> finished_lock = malloc(sizeof(pthread_mutex_t)*threads);//same
	mr -> id_finished = calloc(threads+1, sizeof(int));
	mr -> reduce_finished = calloc(1, sizeof(int)); //not currently used
	mr -> args_array = malloc(*mr->nmaps * sizeof(struct map_args));
	mr -> rargs = malloc(sizeof(struct reduce_args));

//replace with linked list from here down
//counter to limit size, read from front write to back
//pointer to tail updates every write
//delete head after read and update pointer
//when list is empty, write to node and THEN update map_reduce->head pointer
//consume checks if head pointer is null
//no need for locks ever!
	int i;
	for(i = 0; i < threads ;i++) //need to initialize all heads and tails to NULL 
	{
		(mr-> mr_heads)[i] = NULL;
		(mr-> mr_tails)[i] = NULL;
		//int pthread_mutex_init(pthread_mutex_t *(mr->lock_array)[i], NULL); //something like this might be needed
	}
	
	return mr;
}

/* Destroys and cleans up an existing instance of the MapReduce framework */
void
mr_destroy(struct map_reduce *mr)
{		

	free(mr-> mr_heads);
	free(mr-> mr_tails);
	free((int*)mr->id_finished);
	free(mr-> nmaps);
	free((int*)mr-> buffer_count);
	free(mr-> p_array);
	free(mr -> lock_array);
	free(mr-> reduce_finished);
	free(mr->args_array);
	free(mr->rargs);
	free(mr->count_lock);
	free(mr->finished_lock);

	free(mr); //free the structure ptr
}

/* Begins a multithreaded MapReduce operation */
int
mr_start(struct map_reduce *mr, const char *inpath, const char *outpath)
{
	int i;
	int error;
	for (i = 0; i < *mr->nmaps;i++)
	{
		mr->args_array[i].mr = mr;
		if ((mr->args_array[i].infd = open(inpath, O_RDONLY)) == -1) //handle error when opening
			return -1;
		mr->args_array[i].id = i;
		mr->args_array[i].nmaps = *mr->nmaps;
		error = pthread_create(&mr->p_array[i], NULL, (void*) &map_helper, (mr->args_array)+i);
		if (error != 0)
		{
			close(mr->args_array[i].infd);
			return -1;
		}
	}
	mr->rargs->mr = mr;

	mr->rargs->outfd = open(outpath,O_RDWR | O_CREAT | O_TRUNC, S_IWUSR | S_IRUSR);//tbd do this in helper
	mr->rargs->nmaps = *mr->nmaps;
	error = pthread_create(&mr->p_array[*mr->nmaps], NULL, (void*) reduce_helper, mr->rargs);
	if (error != 0)
	{
		close(mr->rargs->outfd);
		return -1;
	}

	return 0;
}

/* Blocks until the entire MapReduce operation is complete */
int
mr_finish(struct map_reduce *mr)
{
	 int r = 0;
	int i;
	void *val = NULL;

	for (i = 0; i < *mr->nmaps;i++) 
	{
		pthread_join(mr->p_array[i], val);
		if (mr->id_finished[i] == -1)
			r++;
		close(mr->args_array[i].infd);
	}
	pthread_join(mr->p_array[*mr->nmaps], val);
	close(mr->rargs->outfd);
	if (mr->id_finished[*mr->nmaps] == -1)
		r++;
	return r; //r = number of error'd threads
}

/* Called by the Map function each time it produces a key-value pair */
int
mr_produce(struct map_reduce *mr, int id, const struct kvpair *kv)
{
	if (kv == NULL)
		return -1; //handle errors
	while(mr->buffer_count[id] >= MR_BUFFER_SIZE)
	{
	}

	pthread_mutex_lock(&(mr->lock_array[id]));


	Node* temp = malloc(sizeof(Node));
	temp->kv = malloc(sizeof(struct kvpair));

	temp->kv->key = malloc(kv->keysz); //this is what full copy should look like
	temp->kv->value = malloc(kv->valuesz);
	memcpy(temp->kv->key, kv->key, kv->keysz);
	memcpy(temp->kv->value, kv->value, kv->valuesz);
	temp->kv->keysz = kv->keysz;
	temp->kv->valuesz = kv->valuesz;


	temp->next = (mr->mr_tails)[id]; //place temp behind the old "tail" / top of stack
	(mr->mr_tails)[id] = temp; //make temp the new tail
	mr->buffer_count[id]++;


	pthread_mutex_unlock(&(mr->lock_array[id]));

	return 1;
}

/* Called by the Reduce function to consume a key-value pair */
int
mr_consume(struct map_reduce *mr, int id, struct kvpair *kv)
{

	while(mr->buffer_count[id] <= 0) //wait until element exists
	{
		if (mr->id_finished[id] != 0) //if mapper has finished
		{
			if (mr->buffer_count[id] <= 0)
				return 0;
		}
	}

	pthread_mutex_lock(&(mr->lock_array[id]));//wait until write is finished

	//printf("%i, %i\n", id, mr->buffer_count[id]);
	mr->buffer_count[id]--;

	Node* temp = mr->mr_tails[id];
	if (temp == NULL)
	{
		printf("ERRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRR\n");
		return 1;
	}
	memcpy(kv->key, temp->kv->key, temp->kv->keysz);
	memcpy(kv->value, temp->kv->value, temp->kv->valuesz);
	kv->keysz = temp->kv->keysz;
	kv->valuesz = temp->kv->valuesz;

	(mr->mr_tails)[id] = (mr->mr_tails)[id]->next; //update tail/top to next, will be null if next is
	free(temp->kv->key);
	free(temp->kv->value);
	free(temp->kv);


	free(temp); //free previous top of stack




	pthread_mutex_unlock(&(mr->lock_array[id]));
	return 1;
}

void map_helper(void* a)
{
	struct map_args * args = (struct map_args*) a;

	int val = (args->mr)->mapfn(args->mr, args->infd, args->id, args->nmaps);
	if(val == 0)
		args->mr->id_finished[args->id] = 1;
	else
		args->mr->id_finished[args->id] = -1;





	// TODO might wanna deal with errors here (ie return val)
}

void reduce_helper(void* a)
{
	struct reduce_args * args = (struct reduce_args*) a;

	int val = (args->mr)->reducefn(args->mr, args->outfd, args->nmaps);
	if(val == 0)
		args->mr->id_finished[*args->mr->nmaps] = 1;
	else
		args->mr->id_finished[*args->mr->nmaps] = -1;
}

