# partial-SPIDER
A partialized version of the SPIDER Algorithm for inclusion dependency detection. The original [SPIDER Algorithm](https://hpi.de/fileadmin/user_upload/fachgebiete/naumann/publications/PDFs/2007_bauckmann_efficiently.pdf) was published by Bauckmann et al. in 2009. The authors discuss the application of partial inclusion dependency (pIND) discovery in section 6 of the paper. They outline a motivation and distinguish between two versions of pIND detection.
1) _The number of all distinct, not included values expressed as a percentage_. Meaning that the distribution of duplicates is disregarded.
2) _The absolute number of not included values_. Meaning the number of entry changes one has to conduct, to clean the pIND to a proper IND.

The authors propose a counting approach for the first version which only affects a small part of their pIND validation code. For the second version the proposed solution is _'To obtain the absolute number of not included values we need the number of occurrences for each value in the dependent attributes, which can be extracted from the database'._ No more attention is given to the problem. In a setting, where the input is a file, there is no easy way to get the number of occurrences for each value, especially if attributes do not fit into main memory.

My implementation solves this issue by treating the number of occurrences as an essential part of any entry. This enables us to answer both versions of the question through the same concept.

## Advantages of this implementation
The original implementation of Bauckmann appears to be lost. This causes me to use the implementation offered by Dürsch et al. through their paper [Inclusion Dependency Discovery: An Experimental Evaluation of Thirteen Algorithms](https://hpi.de/fileadmin/user_upload/fachgebiete/naumann/publications/PDFs/2019_duersch_inclusion.pdf). The code for their SPIDER implementation can be found [on github](https://github.com/HPI-Information-Systems/inclusion-dependency-algorithms/tree/master/adp-algorithms/spider).

### Something about algo runtime

TODO: compare runtime
TODO: Make plot

### Something about clean code

### Ability to detect partial/dirty INDs
There is no other known implementation which offers pIND discovery via the SPIDER Algorithm. The user can set a threshold which is then used to execute the algorithm and find all pINDs that have at most that percentage of violations (using version 1 or 2).

### Null value treatment
This implementation also offers a variety of null value treatments, such that user can pick the version which fits their needs best. TODO: Link main pIND repo

## Disadvantages of this implementation
Currently there is no integration into a framework like [Metanome](https://github.com/HPI-Information-Systems/Metanome). A user would have to execute the algorithm without a visual interface. A Database input is not possible. Meaning a user would need to create a file dump before the execution is possible. These inconveniences exist, since this project only exists as a comparison algorithm for my master thesis. Code adjustments, which would enable such integrations, should be fairly easy.