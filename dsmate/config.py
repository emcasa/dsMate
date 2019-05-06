ALLOWED_STEPS = {
    'data-acquisition': ['dataframe', 'acquisitor'],
    'data-preparation': ['dataframe', 'preparator'],
    'model-training': ['dataframe', 'trainer'],
    'model-evaluation': ['dataframe', 'evaluator', 'results'],
    'model-deployment': ['lambda', 'pickle']
}