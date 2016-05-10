from sklearn.linear_model import Perceptron
from import_data import import_raw_data, get_features_and_target
from data_cleaning import complete_cleaning

train_chunks = import_raw_data('train', by_chunk=True)

model = Perceptron(penalty='l2',
                   alpha=0.0001,
                   fit_intercept=True,
                   n_iter=5000,
                   shuffle=True,
                   random_state=0,
                   verbose=1000,
                   n_jobs=-1,
                   eta0=0.9,
                   warm_start=True,)

for (i, train_chunk) in enumerate(train_chunks):
    print 'Chunk %s' % i + '-'*25
    train_chunk = complete_cleaning(train_chunk)
    X, y = get_features_and_target(train_chunk)
    model.fit(X, y)
