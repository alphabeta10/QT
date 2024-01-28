from sklearn.model_selection import cross_val_score
def select_best_model(functions,trainX,trainY,cv):
    val_dict = {}
    for fun in functions:
        val_score = cross_val_score(fun,trainX,trainY,cv=cv)
        #val_dict[fun] = [val_score.mean(),val_score.std()]
        val_dict[fun] = val_score.std()
    return val_dict