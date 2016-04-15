/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.udf.generic;

import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping.BINARY_GROUP;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping.STRING_GROUP;

import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping;

import java.security.GeneralSecurityException;
import java.security.NoSuchAlgorithmException;

import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;

/**
 * GenericUDFAesBase.
 *
 */
public abstract class GenericUDFAesBase extends GenericUDF {
  private static final String[] ORDINAL_SUFFIXES = new String[] { "th", "st", "nd", "rd", "th",
      "th", "th", "th", "th", "th" };
  protected transient Converter[] converters = new Converter[2];
  protected transient PrimitiveCategory[] inputTypes = new PrimitiveCategory[2];
  protected final BytesWritable output = new BytesWritable();
  protected transient boolean isStr0;
  protected transient boolean isStr1;
  protected transient boolean isKeyConstant;
  protected transient Cipher cipher;
  protected transient SecretKey secretKey;

  public static BytesWritable getConstantBytesValue(ObjectInspector[] arguments, int i) {
    Object constValue = ((ConstantObjectInspector) arguments[i]).getWritableConstantValue();
    return (BytesWritable) constValue;
  }

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    checkArgsSize(arguments, 2, 2);

    checkArgPrimitive(arguments, 0);
    checkArgPrimitive(arguments, 1);

    // the function should support both string and binary input types
    if (canParam0BeStr()) {
      checkArgGroups(arguments, 0, inputTypes, STRING_GROUP, BINARY_GROUP);
    } else {
      checkArgGroups(arguments, 0, inputTypes, BINARY_GROUP);
    }
    checkArgGroups(arguments, 1, inputTypes, STRING_GROUP, BINARY_GROUP);

    if (isStr0 = PrimitiveObjectInspectorUtils.getPrimitiveGrouping(inputTypes[0]) == STRING_GROUP) {
      obtainStringConverter(arguments, 0, inputTypes, converters);
    } else {
      GenericUDFParamUtils.obtainBinaryConverter(arguments, 0, inputTypes, converters);
    }

    isKeyConstant = arguments[1] instanceof ConstantObjectInspector;
    byte[] key = null;
    int keyLength = 0;

    if (isStr1 = PrimitiveObjectInspectorUtils.getPrimitiveGrouping(inputTypes[1]) == STRING_GROUP) {
      if (isKeyConstant) {
        String keyStr = getConstantStringValue(arguments, 1);
        if (keyStr != null) {
          key = keyStr.getBytes();
          keyLength = key.length;
        }
      } else {
        obtainStringConverter(arguments, 1, inputTypes, converters);
      }
    } else {
      if (isKeyConstant) {
        BytesWritable keyWr = GenericUDFParamUtils.getConstantBytesValue(arguments, 1);
        if (keyWr != null) {
          key = keyWr.getBytes();
          keyLength = keyWr.getLength();
        }
      } else {
        GenericUDFParamUtils.obtainBinaryConverter(arguments, 1, inputTypes, converters);
      }
    }

    if (key != null) {
      secretKey = getSecretKey(key, keyLength);
    }

    try {
      cipher = Cipher.getInstance("AES");
    } catch (NoSuchPaddingException | NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }

    ObjectInspector outputOI = PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
    return outputOI;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    byte[] input;
    int inputLength;

    if (isStr0) {
      Text n = GenericUDFParamUtils.getTextValue(arguments, 0, converters);
      if (n == null) {
        return null;
      }
      input = n.getBytes();
      inputLength = n.getLength();
    } else {
      BytesWritable bWr = GenericUDFParamUtils.getBinaryValue(arguments, 0, converters);
      if (bWr == null) {
        return null;
      }
      input = bWr.getBytes();
      inputLength = bWr.getLength();
    }

    if (input == null) {
      return null;
    }

    SecretKey secretKey;
    if (isKeyConstant) {
      secretKey = this.secretKey;
    } else {
      byte[] key;
      int keyLength;
      if (isStr1) {
        Text n = GenericUDFParamUtils.getTextValue(arguments, 1, converters);
        if (n == null) {
          return null;
        }
        key = n.getBytes();
        keyLength = n.getLength();
      } else {
        BytesWritable bWr = GenericUDFParamUtils.getBinaryValue(arguments, 1, converters);
        if (bWr == null) {
          return null;
        }
        key = bWr.getBytes();
        keyLength = bWr.getLength();
      }
      if (keyLength < 16) {
        byte[] bytes = {'\0','\0','\0','\0','\0','\0','\0','\0',
                        '\0','\0','\0','\0','\0','\0','\0','\0'};
        for (int i = 0; i < keyLength; ++i) bytes[i] = key[i];
        keyLength = 16;
        key = bytes;
      }
      secretKey = getSecretKey(key, keyLength);
    }

    if (secretKey == null) {
      return null;
    }

    byte[] res = aesFunction(input, inputLength, secretKey);

    if (res == null) {
      return null;
    }

    output.set(res, 0, res.length);
    return output;
  }

  protected SecretKey getSecretKey(byte[] key, int keyLength) {
    if (keyLength == 16 || keyLength == 32 || keyLength == 24) {
      return new SecretKeySpec(key, 0, keyLength, "AES");
    }
    return null;
  }

  protected byte[] aesFunction(byte[] input, int inputLength, SecretKey secretKey) {
    try {
      cipher.init(getCipherMode(), secretKey);
      byte[] res = cipher.doFinal(input, 0, inputLength);
      return res;
    } catch (GeneralSecurityException e) {
      return null;
    }
  }

  abstract protected int getCipherMode();

  abstract protected boolean canParam0BeStr();

  @Override
  public String getDisplayString(String[] children) {
    return getStandardDisplayString(getFuncName(), children);
  }

  protected void checkArgsSize(ObjectInspector[] arguments, int min, int max)
      throws UDFArgumentLengthException {
    if (arguments.length < min || arguments.length > max) {
      StringBuilder sb = new StringBuilder();
      sb.append(getFuncName());
      sb.append(" requires ");
      if (min == max) {
        sb.append(min);
      } else {
        sb.append(min).append("..").append(max);
      }
      sb.append(" argument(s), got ");
      sb.append(arguments.length);
      throw new UDFArgumentLengthException(sb.toString());
    }
  }

  protected void checkArgPrimitive(ObjectInspector[] arguments, int i)
      throws UDFArgumentTypeException {
    ObjectInspector.Category oiCat = arguments[i].getCategory();
    if (oiCat != ObjectInspector.Category.PRIMITIVE) {
      throw new UDFArgumentTypeException(i, getFuncName() + " only takes primitive types as "
          + getArgOrder(i) + " argument, got " + oiCat);
    }
  }

  protected String getFuncName() {
    return getClass().getSimpleName().substring(10).toLowerCase();
  }

  protected String getArgOrder(int i) {
    i++;
    switch (i % 100) {
    case 11:
    case 12:
    case 13:
      return i + "th";
    default:
      return i + ORDINAL_SUFFIXES[i % 10];
    }
  }

  protected String getStandardDisplayString(String name, String[] children) {
    return getStandardDisplayString(name, children, ", ");
  }

  protected String getStandardDisplayString(String name, String[] children, String delim)
  {
    StringBuilder sb = new StringBuilder();
    sb.append(name);
    sb.append("(");
    if (children.length > 0) {
      sb.append(children[0]);
      for (int i = 1; i < children.length; i++) {
        sb.append(delim);
        sb.append(children[i]);
      }
    }
    sb.append(")");
    return sb.toString();
  }

  protected String getConstantStringValue(ObjectInspector[] arguments, int i) {
    Object constValue = ((ConstantObjectInspector) arguments[i]).getWritableConstantValue
();
    String str = constValue == null ? null : constValue.toString();
    return str;
  }

  protected Integer getConstantIntValue(ObjectInspector[] arguments, int i)
      throws UDFArgumentTypeException {
    Object constValue = ((ConstantObjectInspector) arguments[i]).getWritableConstantValue
();
    if (constValue == null) {
      return null;
    }
    int v;
    if (constValue instanceof IntWritable) {
      v = ((IntWritable) constValue).get();
    } else if (constValue instanceof ShortWritable) {
      v = ((ShortWritable) constValue).get();
    } else if (constValue instanceof ByteWritable) {
      v = ((ByteWritable) constValue).get();
    } else {
      throw new UDFArgumentTypeException(i, getFuncName() + " only takes INT/SHORT/BYTE types as "
          + getArgOrder(i) + " argument, got " + constValue.getClass());
    }
    return v;
  }

  protected void checkArgGroups(ObjectInspector[] arguments, int i, 
    PrimitiveCategory[] inputTypes,
      PrimitiveGrouping... grps) throws UDFArgumentTypeException {
    PrimitiveCategory inputType = ((PrimitiveObjectInspector) arguments[i]).getPrimitiveCategory();
    for (PrimitiveGrouping grp : grps) {
      if (PrimitiveObjectInspectorUtils.getPrimitiveGrouping(inputType) == grp) {
        inputTypes[i] = inputType;
        return;
      }
    }
    StringBuilder sb = new StringBuilder();
    sb.append(getFuncName());
    sb.append(" only takes ");
    sb.append(grps[0]);
    for (int j = 1; j < grps.length; j++) {
      sb.append(", ");
      sb.append(grps[j]);
    }
    sb.append(" types as ");
    sb.append(getArgOrder(i));
    sb.append(" argument, got ");
    sb.append(inputType);
    throw new UDFArgumentTypeException(i, sb.toString());
  }

  protected void obtainStringConverter(ObjectInspector[] arguments, int i,
      PrimitiveCategory[] inputTypes, Converter[] converters) throws UDFArgumentTypeException {
    PrimitiveObjectInspector inOi = (PrimitiveObjectInspector) arguments[i];
    PrimitiveCategory inputType = inOi.getPrimitiveCategory();

    Converter converter = ObjectInspectorConverters.getConverter(
        arguments[i],
        PrimitiveObjectInspectorFactory.writableStringObjectInspector);
    converters[i] = converter;
    inputTypes[i] = inputType;
  }
}
