from optlang import Model, Variable, Constraint, Objective


class Solver:

    @classmethod
    # This method solves for trendline passing through Highs using the concepts of Linear programming
    # returns values of slope and intercept if a trendline exists
    # returns none values of slope and intercept if a trendline does not exists
    def RunH(self, candles):
        model = Model(name="TrendLineSolverForHighs")
        m = Variable("Slope")
        c = Variable("Intercept")
        model.objective = Objective(Solver.SumOfIndexes(
            candles) * m + len(candles) * c, direction='max')
        constraints = []
        for candle in candles:
            constraints.append(Constraint(
                candle.Index * m + c, ub=candle.High))
            constraints.append(Constraint(
                candle.Index * m + c, lb=max(candle.Open, candle.Close)))
        model.add(constraints)
        if (model.optimize() == 'optimal'):
            return [model.variables.Intercept.primal, model.variables.Slope.primal]
        else:
            return [None, None]

    @classmethod
    # This method solves for trendline passing through Lows using the concepts of Linear programming
    # returns values of slope and intercept if a trendline exists
    # returns none values of slope and intercept if a trendline does not exists
    def RunL(self, candles):
        model = Model(name="TrendLineSolverForLows")
        m = Variable("Slope")
        c = Variable("Intercept")
        model.objective = Objective(Solver.SumOfIndexes(
            candles) * m + len(candles) * c, direction='min')
        constraints = []
        for candle in candles:
            constraints.append(Constraint(
                candle.Index * m + c, ub=min(candle.Open, candle.Close)))
            constraints.append(Constraint(candle.Index * m + c, lb=candle.Low))
        model.add(constraints)
        if (model.optimize() == 'optimal'):
            return [model.variables.Intercept.primal, model.variables.Slope.primal]
        else:
            return [None, None]

    @classmethod
    # This method returns the sum of indexes of the candles
    def SumOfIndexes(self, candles):
        sum = 0
        for candle in candles:
            sum += candle.Index
        return sum

    @classmethod
    # This method returns slope
    def SlopeRange(self, candle1, candle2, H_L):
        if (H_L == "H"):
            return [(candle2.High-max(candle1.Open, candle1.Close))/(candle2.Index-candle1.Index), (candle1.High-max(candle2.Open, candle2.Close))/(candle1.Index-candle2.Index)]
        elif (H_L == "L"):
            return [(candle1.Low-min(candle2.Open, candle2.Close))/(candle1.Index-candle2.Index), (candle2.Low-min(candle1.Open, candle1.Close))/(candle2.Index-candle1.Index)]
