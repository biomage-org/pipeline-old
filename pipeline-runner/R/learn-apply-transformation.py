import numpy as np
from scanorama import transform
from sklearn.neighbors import NearestNeighbors

# Apply integrated coordinates back to full data.
def apply_transf(unsketched_data, sketched_data, sketched_integrated_data):
    labels = []
    curr_label = 0
    for i, a in enumerate(sketched_data):
        labels += list(np.zeros(a.shape[0]) + curr_label)
        curr_label += 1
    labels = np.array(labels, dtype=int)

    for i, (X_unsketch, X_sketch) in enumerate(zip(unsketched_data, sketched_data)):
        X_int = sketched_integrated_data[i]

        neigh = NearestNeighbors(n_neighbors=3).fit(X_unsketch)
        _, neigh_idx = neigh.kneighbors(X_sketch)

        ds_idxs, ref_idxs = [], []
        for ref_idx in range(neigh_idx.shape[0]):
            for k_idx in range(neigh_idx.shape[1]):
                ds_idxs.append(neigh_idx[ref_idx, k_idx])
                ref_idxs.append(ref_idx)

        bias = transform(X_unsketch, X_int, ds_idxs, ref_idxs, 15, batch_size=1000)

        sketched_integrated_data[i] = X_unsketch + bias

    return sketched_integrated_data