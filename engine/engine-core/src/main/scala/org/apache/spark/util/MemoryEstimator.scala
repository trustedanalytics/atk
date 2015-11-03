/**
 *  Copyright (c) 2015 Intel Corporation 
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */


package org.apache.spark.util

import java.lang.reflect.{ Array, Modifier }
import java.util

import sun.misc.Unsafe

/**
 * Helper function for computing the field offset
 */
class FieldOffsetHelper {
  var b: Byte = 0
}

object MemoryEstimator extends MemoryEstimator

/**
 * Class for estimating the size of a Java object
 *
 * @see http://java-performance.info/memory-introspection-using-sun-misc-unsafe-and-reflection/
 */
class MemoryEstimator {

  /** The detected runtime address mode. */
  val field = classOf[Unsafe].getDeclaredField("theUnsafe")
  field.setAccessible(true)
  private val UNSAFE: Unsafe = field.get(null).asInstanceOf[Unsafe]
  private val HELPER_OBJECT: AnyRef = new FieldOffsetHelper()
  private val OBJECT_BASE_SIZE: Long = UNSAFE.objectFieldOffset(HELPER_OBJECT.getClass.getDeclaredField("b"))
  private val OBJECT_ALIGNMENT: Long = 8

  /** Return the size of the object excluding any referenced objects. */
  def shallowSize(javaObject: AnyRef): Long = {
    var objectClass: Class[_] = javaObject.getClass
    if (objectClass.isArray) {
      val size: Long = UNSAFE.arrayBaseOffset(objectClass) + UNSAFE.arrayIndexScale(objectClass) * Array.getLength(javaObject)
      padSize(size)
    }
    else {
      var size: Long = OBJECT_BASE_SIZE
      do {
        for (field <- objectClass.getDeclaredFields) {
          if ((field.getModifiers & Modifier.STATIC) == 0) {
            val offset: Long = UNSAFE.objectFieldOffset(field)
            if (offset >= size) {
              size = offset + 1
            }
          }
        }
        objectClass = objectClass.getSuperclass
      } while (objectClass != null)
      padSize(size)
    }
  }

  /** Return the size of the object including any referenced objects. */
  def deepSize(javaObject: AnyRef): Long = {
    val visited: util.IdentityHashMap[AnyRef, AnyRef] = new util.IdentityHashMap[AnyRef, AnyRef]
    val stack: util.Stack[AnyRef] = new util.Stack[AnyRef]
    if (javaObject != null) stack.push(javaObject)
    var size: Long = 0
    while (!stack.isEmpty) {
      size += internalSizeOf(stack.pop, stack, visited)
    }
    size
  }

  private def padSize(size: Long): Long = {
    (size + (OBJECT_ALIGNMENT - 1)) & ~(OBJECT_ALIGNMENT - 1)
  }

  private def internalSizeOf(javaObject: AnyRef, stack: util.Stack[AnyRef], visited: util.IdentityHashMap[AnyRef, AnyRef]): Long = {
    var c: Class[_] = javaObject.getClass
    if (c.isArray && !c.getComponentType.isPrimitive) {
      {
        var i: Int = Array.getLength(javaObject) - 1
        while (i >= 0) {
          val value: AnyRef = Array.get(javaObject, i)
          if (value != null && visited.put(value, value) == null) {
            stack.add(value)
          }
          i -= 1
        }
      }
    }
    else {
      while (c != null) {
        {
          for (field <- c.getDeclaredFields) {
            if ((field.getModifiers & Modifier.STATIC) == 0 && !field.getType.isPrimitive) {
              field.setAccessible(true)
              try {
                val value: AnyRef = field.get(javaObject)
                if (value != null && visited.put(value, value) == null) {
                  stack.add(value)
                }
              }
              catch {
                case e: IllegalArgumentException =>
                  throw new RuntimeException(e)
                case e: IllegalAccessException =>
                  throw new RuntimeException(e)
              }
            }
          }
        }
        c = c.getSuperclass
      }
    }
    shallowSize(javaObject)
  }
}
