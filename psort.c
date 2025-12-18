/***********************************************
 * Projet 4 : Tri parallèle
 * 
 * Numéro de groupe : 3
 * Étudiants     : Maxime Lawson , Thomas Colangelo
 * 
 * Si vous utilisez plusieurs fichiers, expliquez ici comment compiler votre
 * programme : /
 * 
 * 
 *
 * Expliquez ici le fonctionnement de votre algorithme de tri,
 * et comment il est parallélisé (mentionnez vos sources si pertinent) :
 * 
 * //https://www.youtube.com/watch?v=bOk35XmHPKs
 * 
 * L'algorithme de tri est celui du tri par fusion.
 * 
 * Ce tri est réalisé sur un tableau d’indices plutôt que directement sur les données mappées *en mémoire.
 * Chaque indice représente la position d’une entrée dans le fichier d’entrée, et les comparaisons sont *effectuées uniquement sur les clés de 4 octets associées à ces indices.
 * Cette approche permet d’éviter de recopier les entrées complètes de 100 octets durant le tri, réduisant ainsi les accès mémoire et le coût des copies.
 * L’algorithme utilise un système de bornes gauche et droite (left et right) qui évoluent récursivement lors des appels à la fonction de tri, 
 * ce qui permet de travailler sur différentes portions du tableau d’indices sans dupliquer l’ensemble du tableau.
 * Les données originales mappées avec mmap ne sont jamais modifiées pendant le tri et ne sont copiées dans le fichier de sortie qu’une seule fois, 
 * une fois l’ordre final des indices déterminé.
 * 
 * Vu que chaque thread travaille avec une partie différente du tableau d'indice, on a pas besoin d'implémenter un mutex
 * puisqu'il n'y a pas de risques que un thread trie des données qui sont en même temps triées par un autre thread.
 * 
 * On a observé que le nombre maximum de threads pour que ça soit encore bénéfique d'ajouter des threads est 4,
 * au delà ou en deçà de ce nombre de threads, le temps d'exécution augmente.
 * 
 ***********************************************/

#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <string.h>
#include <pthread.h>


#define ENTRY_SIZE 100
#define KEY_SIZE 4

int compare_keys(unsigned char *keys, int idx1, int idx2) { 
    
    // Adresse de la clé idx1 et clé idx2
    unsigned char *adressePremiereClef = keys + idx1 * KEY_SIZE; //1234 + '2' * 4 = 1242   si clef d'idx '2'
    unsigned char *adresseDeuxiemeClef = keys + idx2 * KEY_SIZE; //1234 + '4' * 4 = 1250   si clef d'idx '4'

    // Comparaison byte par byte des 4 octets, comparaison lexicographique de gauche à droite.
    // Compare clef 1 par rapport a clef 2, si clef 1 < clef 2 return <0 si egales =0 si > alors >0
    return memcmp(adressePremiereClef, adresseDeuxiemeClef, KEY_SIZE);
}

/*
    Va copier le tableau d'indices de gauche au milieu pour L ou
    du milieu à droite pour R.
*/
int* copy_indices(int *indices, int start, int end) { 
    int n = end - start + 1; //n = le nbr d'élément à copier
    int *arr = malloc(n * sizeof(int));  // on utilise sizeof(int) car si écrit en brut ca peut changer d'un pc à l'autre
    if (!arr) { perror("malloc"); exit(1); }
    for (int i = 0; i < n; i++)
        arr[i] = indices[start + i];
    return arr;
}

void merge_indices(int *indices,int *L, int n1,int *R, int n2,int left,unsigned char *keys){
    int i = 0;      // index dans L
    int j = 0;      // index dans R
    int k = left;  // index dans indices

    while (i < n1 && j < n2) {//on reste dans la boucle tant que L ou R a toujours des éléments

        int res = compare_keys(keys, L[i], R[j]);
        if (res <= 0) {
            indices[k] = L[i];
            i++;
        } else {
            indices[k] = R[j];
            j++;
        }
        k++;
    }
    // Si l’un des deux tableaux triés est épuisé, il suffit de copier le reste de l’autre tableau.
    while (i < n1) {indices[k] = L[i];i++;k++;}
    while (j < n2) {indices[k] = R[j]; j++; k++;}
}//Ici on ecris dans indices[k] en écrasant les données parce qu'elles sont transferées avant dans les tableaux temporaires L et R

void merge(int *indices, int left, int mid, int right, unsigned char *keys) {
    int *L = copy_indices(indices, left, mid); // L contient la partie gauche
    int *R = copy_indices(indices, mid + 1, right);// R contient la partie droite 
    merge_indices(indices, L, mid - left + 1, R, right - mid, left, keys); 
    free(L);
    free(R);
}

void merge_sort(int *indices, int left, int right, unsigned char *keys) {
    if (left >= right) return; // rien à trier/split si 0 ou 1 élément
    int mid = (left + right) / 2;
    merge_sort(indices, left, mid, keys);
    merge_sort(indices, mid + 1, right, keys);
    merge(indices, left, mid, right, keys);
}

/*On crée une structure parce que pthread_create n’autorise qu’un seul argument,
 donc on regroupe tous les paramètres du thread dans un seul objet.*/
typedef struct {
    int *indices;          // Tableau d’indices à trier
    unsigned char *keys;   // Tableau des clés (lecture seule)
    int left;              // Borne gauche de la portion à trier
    int right;             // Borne droite de la portion à trier
} thread_arg_t;

void *thread_sort(void *arg) { // Trie une portion du tableau d’indices avec merge_sort(chauqe thread trie sa partie)
    thread_arg_t *t = (thread_arg_t *)arg;
    merge_sort(t->indices, t->left, t->right, t->keys);
    return NULL;
} 

int main(int argc, char **argv) {
    //oblige le programme à se lancer avec 4 arguments : le nom du programme, l'input, l'output et le nombre de threads max
    if (argc != 4) { 
        fprintf(stderr, "Usage: %s input output nb_threads\n", argv[0]);
        return 1;
    }

    //nombre de threads maximum
    int nb_threads = atoi(argv[3]); //atoi = ASCII TO INTEGER

    if (nb_threads < 1) {
        fprintf(stderr, "Error: nb_threads should be >= 1\n");
        return 1;
    }

    const char *input_filename = argv[1]; // fichier d'input
    const char *output_filename = argv[2]; // fichier d'output 

    // 1) OUVERTURE DE L’INPUT

    int fd_in = open(input_filename, O_RDONLY);
    if (fd_in == -1) { perror("open input"); return 1;}

    // 2) LIRE LA TAILLE DU FICHIER

    struct stat st;
    if (fstat(fd_in, &st) == -1) { perror("fstat"); close(fd_in); return 1;}
    size_t filesize = st.st_size;

    if (filesize % ENTRY_SIZE != 0) {
        fprintf(stderr, "Error: corrupted file (not a multiple of 100 bytes)\n");
        close(fd_in);
        return 1;
    }

    size_t n_entries = filesize / ENTRY_SIZE;

    int nb_used_threads = nb_threads; // nombre réels de threads utilisés comme expliquer dans les commentaires d'en-tête
    if(nb_used_threads > 4)
        nb_used_threads = 4;
        
    if (n_entries < (size_t)nb_used_threads) { // cas où il y a plus de threads que d'entrées
        nb_used_threads = n_entries;
    }

    // 3) MMAP DU FICHIER D’ENTRÉE

    unsigned char *data_in = mmap(NULL, filesize, PROT_READ, MAP_SHARED, fd_in, 0);
    if (data_in == MAP_FAILED) { perror("mmap input"); close(fd_in); return 1;}

    // 4) BUFFER DES CLÉS (4 bytes par entrée)
    unsigned char *keys = malloc(n_entries * KEY_SIZE);
    if (!keys) { perror("malloc keys");munmap(data_in, filesize);close(fd_in);return 1;}

    // Copier les 4 bytes de chaque entrée

    for (size_t i = 0; i < n_entries; i++) {
        memcpy(keys + i*KEY_SIZE, data_in + i*ENTRY_SIZE, KEY_SIZE);
    }

    // 5) TABLEAU DES INDICES (0, 1, 2, ..., n_entries-1)

    int *indices = malloc(n_entries * sizeof(int));
    if (!indices) { perror("malloc indices"); free(keys); munmap(data_in, filesize); close(fd_in); return 1;}

    // Remplissage du tableau d'indices 
    for (size_t i = 0; i < n_entries; i++) {
        indices[i] = i;
    }

    // 6) TRI PAR FUSION (sur les indices)

    if (nb_used_threads == 1) {
        // Version séquentielle
        merge_sort(indices, 0, n_entries - 1, keys);
    } else {
        // Version multithread
        pthread_t threads[32];// tableau de thread
        thread_arg_t args[32];// tableau de structure contenant les argument que chaque thread a besoin pour trier

        int base = n_entries / nb_used_threads;
        int reste = n_entries % nb_used_threads;
        int start = 0;

        for (int i = 0; i < nb_used_threads; i++) {
            int size = base + (i < reste ? 1 : 0);

            // Préparer args[i] avec les valeurs pour le thread
            args[i].indices = indices;
            args[i].keys = keys;
            args[i].left = start;
            args[i].right = start + size - 1;

            pthread_create(&threads[i], NULL, thread_sort, &args[i]);

            start += size;
        }

        for (int i = 0; i < nb_used_threads; i++) { // une boucle qui attend que chaque thread se termine 
            pthread_join(threads[i], NULL);
        }

        // fusion finale
        int current_size = base + (0 < reste ? 1 : 0);
        for (int i = 1; i < nb_used_threads; i++) {
            int next_size = base + (i < reste ? 1 : 0);
            merge(indices, 0, current_size - 1, current_size + next_size - 1, keys);
            current_size += next_size;
        }
    }


    // 7) CRÉER LE FICHIER DE SORTIE

    int fd_out = open(output_filename, O_RDWR | O_CREAT | O_TRUNC, 0666);
    if (fd_out == -1) {perror("open output");free(indices);free(keys);munmap(data_in, filesize);close(fd_in);return 1;}

    // Ajuster la taille du fichier de sortie

    if (ftruncate(fd_out, filesize) == -1) {perror("ftruncate");close(fd_out);free(indices);free(keys);munmap(data_in, filesize);
        close(fd_in);return 1;}

    // 8) MMAP DU FICHIER DE SORTIE

    unsigned char *data_out = mmap(NULL, filesize, PROT_READ | PROT_WRITE,
                                   MAP_SHARED, fd_out, 0);
    if (data_out == MAP_FAILED) {perror("mmap output");close(fd_out);free(indices);free(keys);munmap(data_in, filesize);
        close(fd_in);
        return 1;
    }

    // 9) COPIE Ordonnée DANS LE FICHIER OUTPUT

    for (size_t i = 0; i < n_entries; i++) {
        int idx = indices[i];  // entrée originale à la position idx
        memcpy(data_out + i*ENTRY_SIZE, data_in  + idx*ENTRY_SIZE, ENTRY_SIZE);
    }

    // 10) FSYNC POUR FORCER L’ÉCRITURE SUR DISQUE
    if (fsync(fd_out) == -1) { perror("fsync");}

    // 11) LIBÉRATIONS
    munmap(data_out, filesize);
    close(fd_out);
    free(indices);
    free(keys);
    munmap(data_in, filesize);
    close(fd_in);
    return 0;
}