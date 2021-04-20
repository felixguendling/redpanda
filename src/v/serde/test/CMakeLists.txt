rp_test(
  UNIT_TEST
  BINARY_NAME serde
  SOURCES serde_test.cc
  DEFINITIONS BOOST_TEST_DYN_LINK SERDE_TEST
  LIBRARIES v::seastar_testing_main v::serde
  LABELS serde
)
