# Data Complications

A forward note, this text will use 'meaning' to refer to the actual real-word semantic meaning of something. That is the representation of some business state or real-world fact or statistic. The word 'value' will refer to underlying data and its type, such as a string value or instance of an object. The term 'raw binary' refers to the baseline 'bits' that store the 'value' itself.

Any `code` formatting that starts with `0x` is a hexadecimal representation of raw binary, where the characters after the `x` are the number. Similarly `0b` prefixed code formatting is a direct binary representation.

## Semantics

### "Non-Values"

One of the more complicative parts when building reliable data products is dealing with various types of "non-values".
These are representations in the data that indicate that the represented semantic has no useful meaning or quantity.
This most obviously comes up in the case of a `NULL` value in a database or a JSON field value, however that's not the only kind of 'non-value' that can exist.

For strings you can have the empty string `''` -- this is a string, it exists and it has a value, but the value is empty.

For databases and object types you can then have a proper `NULL` where the field exists, but for this instance the value does not exist at all.
It isn't a `NaN` or an empty string `''`, there is simply no value whatsoever of any kind.
This is common in relational queries where a left join did not bring in data from the 'right' table.

In JSON-style structured objects we also have the concept of *undefined* fields.
This means that the field itself does not exist at all in the current object structure, so the question of whether it even has a value does not arise in the first place.

#### Examples

Imagine some data that gets information about a client, which could be a business or an individual.
Some fields may or may not be populated, or may or may not be relevant.
The different options of non/presence and non/value may mean different things.

Individuals can have an emergency contact number, and this person has provided one on the relevant form...

```json
{
    "client_id": "217000",
    "type": "individual",
    "emergency_contact_number": "0491 573 770"
}
```


This person did not provide one on the relevant form...

```json
{
    "client_id": "217001",
    "type": "individual",
    "emergency_contact_number": ""
}
```

This person has not yet filled out the form, so there is no information on what that number might be...

```json
{
    "client_id": "217002",
    "type": "individual",
    "emergency_contact_number": null
}
```

And the concept of an emergency contact does not apply to business clients at all...

```json
{
    "client_id": "217003",
    "type": "business"
}
```

## Surprise Characters

There are a variety of characters waiting to surprise you during text parsing.
Text encoding has a long history and is used for a lot of different things that require a variety of features.
Many of those features are designed for typesetting and visual niceties, but cause unexpected and difficult-to-see issues during parsing with code.

When our data sources may include text from web services, user-entered text from mobile devices, text copied from document writers, all sorts of unusual characters can get embedded into the data.

### En-Dashes, Em-Dashes, and Hyphens

Do not mix these up.
Each line below uses a different dash.

* `a-z` ; a-z; `0x2d` in UTF-8 and ASCII
* `a–z` ; a–z; `0xe28093` in UTF-8, `0x96` in Windows-1252 (does not appear in ASCII)
* `a—z` ; a—z; `0xe28094` in UTF-8, `0x97` in Windows-1252 (does not appear in ASCII)

Modern editors and fonts will show slight visual differences in the symbols, however some editors and fonts do not.­

### Non/Breaking Spaces­­

Do not mix these up.
Each line below uses a different space.

* Guess which space is used where.
* Guess which space is used where.

One line uses the standard space `0x20` and the other uses `0xc2a0` UTF-8 (`0xA0` Windows-1252). This will not be apparent in a text editor, the two of them look *exactly* identical, they are just treated different by renderers for word wrap behaviour.

### 'Soft' Characters

Some characters are only visible when they are used for word wrap.
The 'soft-hyphen' for example indicates where a word break with a hyphen should occur to allow wrapping if required, but otherwise is not displayed at all.
Similarly the 'zero-width-space' cannot be seen until a word-wrap is required, at which point a renderer will break at the indicated character.
Even if you cannot see it, the character *is* there and will be part of copy+paste data.

* You­cannot­see­this­hyphen­unless­you­make­the­window­very­narrow
* You​cannot​see​this​space​unless​you​make​the​windowvery​narrow

The soft hyphen is `0xc2ad` in UTF-8. The non-breaking space is `0xe2808b` in UTF-8.

To see it in action, run this line of code in python and validate the result.

```py
​​​​​​​​​​​​​​​​​​​​​len("​​​​​​​​​​​​​​​​​​​​​​​​​​​​​​​​​​​​​​​​​​")
> 42
```

### Stylised Punctuation

Certain types of punctuation can have alternative characters to show them as left or right handed versions, or otherwise have a slightly different visual style.
These sorts of characters are often automatically substituted in by document editors when typed adjacent to a word.

* `‘’` ; ‘’ are left and right apostrophes.
* `“”` ; “” are left and right double-quotes.

### Ligatures

Ligatures are symbols that have combined two or more other symbols into one.
Some editors will automatically create ligatures if you write the base components sequentially.
This causes the characters to appear when someone has done a copy+paste out of such an editor, and if you write the individual characters.

* ← for <- (and similarly for -> to →)
* ‽ for !? (aka the interrobang)
* … for ... (ellipsis, these ARE different characters)
* "，" for ", " (quotes are used to note the included space)

Renderers may also automatically substitute or otherwise have the symbols appear visually the same.
As above ... (`0x2e` three times) may appear identical to … (`0xe280a6` UTF-8, once).

### Emoji :smile:

Text that is encoded in UTF-8 can have emoji characters as *actual characters* in the string data.
If a data source uses UTF-8 encoding it *does* support native emoji characters, and they *can* appear in the data even if the app or tool used to interact with it cannot *render* them.

Many editors allow you to write emoji codes that will be *substituted* for emoji when rendered, so a copy+paste will get the actual emoji character, but the original underlying source text is plain ASCII-compatible.

In the source code for this document:

* This is a code :fire: (:fir​e:)*
* This is an actual emoji character *in the source* 🔥

This also works in programming languages, for example in Python:

```py
python_dict = { "🔥": "🚒" }
python_dict["🔥"]
> '🚒'
```

\* A zero-width space has been inserted to disrupt the renderer from parsing the emoji code. Can you find it?

## Encoding Messes

Many Windows editors default to reading text in Windows-1252 encoding, or will read in UTF-8 but then convert to Windows-1252 on save only for it to be read back as UTF-8.
This can cause an absolute nightmare of garbled text if any characters beyond the standard ASCII set are used.

There are a variety of ways a simple right apostrophe `’` can turn into a garbled mess:

* `â€™` : `’` encoded in UTF-8 and decoded in W-1252
* `0xe280x99` : `â€™` encoded in  W-1252 and decoded in UTF-8 (unrepresentable other than `0xe2`=`â`)
* `0x92` : `’` encoded in W-1252 and decoded in UTF-8 (unrepresentable)

This is just for `’` -- the same issues can happen with each of `‘’“”•–—` which appear in `0x91-0x97` in W-1252.
There are even more control characters and non-printing characters that will turn into a mess as incorrect encoding/decoding are applied.

## Layers of Meaning

In data spaces we need to deal with multiple layers of meaning and representation.
Each one exists as a different form of physical or logical state.
We need to ensure that as we are moving and transforming data we do not change or create ambiguity in higher orders of meaning. Those orders of meaning are loosely:

Binary -> Structure -> Type -> Semantic

There is another orthogonal concept of *representation*.
We often cannot directly communicate in lower orders of meaning, so we must instead use a representation. For example this document cannot include raw binary, it must instead use encoded text that is a representation of that binary.

#### Binary

This is the raw bits as physically stored or transmitted.
Binary itself has no inherent meaning, it is just a series of off|on or 0|1 markers.
Binary can only take on meaning when it is interpreted through some form of encoding.

For example, what does this mean, what even is it?

`0b01100100011000011011010011000001`

#### Structure

We get software to interpret and use the raw binary data it has available using a particular structure.
With the above binary, we have a few basic structures it could be in that would resolve to different higher-order values.

* Integer (big-endian): `1684108385`
* Integer (little-endian): `1635017060`
* Float: `2.8175145550653004e+20`
* Byte Array: `[100, 97, 116, 97]`

#### Type

Applying a Type to a Structure gives it another higher order of meaning.
Some of these types are quite direct, for example a `UInt32` on the above big-endian gives the *number* `1684108385`.

Other types may give the underlying structure a different meaning.
For example a `datetime` type would instead give the the date and time of `2021-10-23T19:24:20Z` (as written in [ISO8601](https://en.wikipedia.org/wiki/ISO_8601) format), if the `datetime` type stores its data as a big-endian `Int32` of Unix time at 1-second granularity.

Another structure may be some form of string encoding.
There are many string encodings that can be applied to data that will result in different higher order values. Taking our binary from above and using different decodings gets these values...

* UTF-16-LE (little-endian): `慤慴`
* UTF-16-BE (big-endian): `摡瑡`
* UTF-8: `data`
* Windows-1252: `data`
* ASCII: `data`

Note the last three get the same value, because [Windows-1252](https://en.wikipedia.org/wiki/Windows-1252) is one of many [code pages](https://en.wikipedia.org/wiki/Code_page) that extend the 128 code points in 7-bit [ASCII](https://en.wikipedia.org/wiki/ASCII), that [UTF-8](https://en.wikipedia.org/wiki/UTF-8) was specifically designed to be compatible with.

#### Semantic

Even with a specific value available, it does not inherently *mean* anything.
What does the date and time `2021-10-23T19:24:20Z` *mean* in the context of the data we find it in.
Alternatively what does the word `data` mean here?
In this case, in the context of the document, it was picked arbitrarily because its UTF-8 encoding has 4 bytes and that could be reused as an Int32 or a float for demonstration purposes.

With real data though the context in which a type value is placed will convey actual real-world meaning.
Here that is referred to as its 'semantic' value, an actual logical *meaning* to the underlying data.
An example for real world data would be a field called "date_of_birth" that gives an underlying 'date' type the semantic meaning of "the date this person was born".

When we are doing data exchange with de/serialisation steps, we need to think about how we can preserve not just the plain data, but its type *and* semantics. For example, in a CSV file what would `20211023192420` *mean*? It could be an ID value, or it could be the date and time shown above but in yyyymmddHHMMSS format.

Often it is preserving this semantic information that is most difficult in data interchanges.
