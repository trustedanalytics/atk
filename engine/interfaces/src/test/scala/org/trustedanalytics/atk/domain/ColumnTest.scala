/**
 *  Copyright (c) 2016 Intel Corporation 
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
package org.trustedanalytics.atk.domain

import org.trustedanalytics.atk.domain.schema.Column
import org.scalatest.WordSpec
import org.trustedanalytics.atk.domain.schema.DataTypes._

class ColumnTest extends WordSpec {

  "Column" should {
    "allow alpha-numeric with underscores for column names" in {
      Column("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890_", str)
    }

    "not allow - in column names" in {
      intercept[IllegalArgumentException] {
        Column("a-b", str)
      }
    }

    "not allow ' in column names" in {
      intercept[IllegalArgumentException] {
        Column("a'b", str)
      }
    }

    "not allow ? in column names" in {
      intercept[IllegalArgumentException] {
        Column("a?b", str)
      }
    }

    "not allow | in column names" in {
      intercept[IllegalArgumentException] {
        Column("a|b", str)
      }
    }

    "not allow . in column names" in {
      intercept[IllegalArgumentException] {
        Column("a.b", str)
      }
    }

    "not allow ~ in column names" in {
      intercept[IllegalArgumentException] {
        Column("a~b", str)
      }
    }

    "not allow spaces in column names" in {
      intercept[IllegalArgumentException] {
        Column("a b", str)
      }
    }

    "not allow empty column names" in {
      intercept[IllegalArgumentException] {
        Column("", str)
      }
    }

    "not allow null column names" in {
      intercept[IllegalArgumentException] {
        Column(null, str)
      }
    }

    "not allow null column types" in {
      intercept[IllegalArgumentException] {
        Column("valid_name", null)
      }
    }
  }
}
