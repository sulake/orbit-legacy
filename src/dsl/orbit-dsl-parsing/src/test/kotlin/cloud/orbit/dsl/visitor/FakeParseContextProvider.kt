/*
 Copyright (C) 2015 - 2019 Electronic Arts Inc.  All rights reserved.
 This file is part of the Orbit Project <https://www.orbit.cloud>.
 See license in LICENSE.
 */

package cloud.orbit.dsl.visitor

import cloud.orbit.dsl.ast.ParseContext
import org.antlr.v4.runtime.Token

object FakeParseContextProvider : ParseContextProvider {
    val fakeParseContext = ParseContext("", 0, 0)

    override fun fromToken(token: Token): ParseContext = fakeParseContext
}
