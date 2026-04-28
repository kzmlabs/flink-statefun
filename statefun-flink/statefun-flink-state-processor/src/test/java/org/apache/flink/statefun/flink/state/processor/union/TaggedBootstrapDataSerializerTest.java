// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.statefun.flink.state.processor.union;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.typeutils.SerializerTestBase;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.BooleanSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.statefun.sdk.Address;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.testutils.DeeplyEqualsChecker;
import org.junit.jupiter.api.Disabled;

public class TaggedBootstrapDataSerializerTest extends SerializerTestBase<TaggedBootstrapData> {

  private static final List<TypeSerializer<?>> TEST_PAYLOAD_SERIALIZERS =
      Arrays.asList(IntSerializer.INSTANCE, BooleanSerializer.INSTANCE);
  private static final Map<Class<?>, Integer> TYPE_TO_UNION_INDEX = new HashMap<>(2);

  static {
    TYPE_TO_UNION_INDEX.put(Integer.class, 0);
    TYPE_TO_UNION_INDEX.put(Boolean.class, 1);
  }

  public TaggedBootstrapDataSerializerTest() {
    super(
        new DeeplyEqualsChecker()
            .withCustomCheck(
                (o1, o2) -> o1 instanceof TaggedBootstrapData && o2 instanceof TaggedBootstrapData,
                (o1, o2, checker) -> {
                  TaggedBootstrapData obj1 = (TaggedBootstrapData) o1;
                  TaggedBootstrapData obj2 = (TaggedBootstrapData) o2;
                  return obj1.getTarget().equals(obj2.getTarget())
                      && obj1.getUnionIndex() == obj2.getUnionIndex()
                      // equality checks on payload makes sense here since
                      // the payloads are only booleans or integers in this test
                      && obj1.getPayload().equals(obj2.getPayload());
                }));
  }

  @Override
  protected TaggedBootstrapData[] getTestData() {
    final TaggedBootstrapData[] testData = new TaggedBootstrapData[3];
    testData[0] = integerPayloadBootstrapData("test-namespace", "test-name", "test-id-1", 1991);
    testData[1] = booleanPayloadBootstrapData("test-namespace", "test-name-2", "test-id-80", false);
    testData[2] = integerPayloadBootstrapData("test-namespace", "test-name", "test-id-56", 1108);

    return testData;
  }

  private TaggedBootstrapData integerPayloadBootstrapData(
      String functionNamespace, String functionName, String functionId, int payload) {
    return new TaggedBootstrapData(
        addressOf(functionNamespace, functionName, functionId),
        payload,
        TYPE_TO_UNION_INDEX.get(Integer.class));
  }

  private TaggedBootstrapData booleanPayloadBootstrapData(
      String functionNamespace, String functionName, String functionId, boolean payload) {
    return new TaggedBootstrapData(
        addressOf(functionNamespace, functionName, functionId),
        payload,
        TYPE_TO_UNION_INDEX.get(Boolean.class));
  }

  @Override
  protected TypeSerializer<TaggedBootstrapData> createSerializer() {
    return new TaggedBootstrapDataSerializer(TEST_PAYLOAD_SERIALIZERS);
  }

  @Override
  protected Class<TaggedBootstrapData> getTypeClass() {
    return TaggedBootstrapData.class;
  }

  @Override
  protected int getLength() {
    return -1;
  }

  // -----------------------------------------------------------------------------
  //  Ignored tests
  // -----------------------------------------------------------------------------

  @Override
  @Disabled
  public void testConfigSnapshotInstantiation() {
    // test ignored; this is a test that is only relevant for serializers that are used for
    // persistent data
  }

  @Override
  @Disabled
  public void testSnapshotConfigurationAndReconfigure() {
    // test ignored; this is a test that is only relevant for serializers that are used for
    // persistent data
  }

  // -----------------------------------------------------------------------------
  //  Utilities
  // -----------------------------------------------------------------------------

  private static Address addressOf(
      String functionNamespace, String functionName, String functionId) {
    return new Address(new FunctionType(functionNamespace, functionName), functionId);
  }
}
