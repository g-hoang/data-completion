import torch

class SimilarityDataset(torch.utils.data.Dataset):
    def __init__(self, encodings, cluster, matching_ids):
        self.encodings = encodings
        self.cluster = cluster
        self.matching_ids = matching_ids

    def __getitem__(self, idx):
        item = {key: torch.tensor(val[idx]) for key, val in self.encodings.items()}
        item['cluster'] = torch.tensor(self.cluster[idx])
        item['matching_ids'] = torch.tensor(self.matching_ids[idx])
        return item

    def __len__(self):
        return len(self.cluster)


class IterableSimilarityDataset(torch.utils.data.IterableDataset):
    def __init__(self, encodings, cluster, matching_ids):
        self.encodings = encodings
        self.cluster = cluster
        self.matching_ids = matching_ids

    def __iter__(self):
        for idx in range(0, self.__len__()):
            item = {key: torch.tensor(val[idx]) for key, val in self.encodings.items()}
            item['cluster'] = torch.tensor(self.cluster[idx])
            item['matching_ids'] = torch.tensor(self.matching_ids[idx])

            yield item

    def __len__(self):
        return len(self.cluster)
