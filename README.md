# CAVES - Client Analysis, Verification and Experimentation System 

Testing stuff:

If you have two interfaces in the SDK (for instance, Sync vs Async), the SDK
must only run one tests against the test framework with the variant which is
closest to the IO later, and the other interface should be tested internally
as tests which confirm the higher level calls the lower level as expected.
