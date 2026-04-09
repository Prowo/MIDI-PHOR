# Example MIDI

## Bundled demo (`lmd_dancing_queen.mid`)

The Hugging Face / Gradio **Example** button uses **`lmd_dancing_queen.mid`**, taken from the **Lakh MIDI Dataset** [Clean MIDI subset](https://colinraffel.com/projects/lmd/) (artist/title layout: `ABBA/Dancing Queen.mid` in the published archive).

### License and attribution (CC-BY 4.0)

The Lakh MIDI Dataset is distributed under [**CC-BY 4.0**](https://creativecommons.org/licenses/by/4.0/). If you use this data, cite the dataset and the author’s thesis:

- Dataset page: [The Lakh MIDI Dataset v0.1](https://colinraffel.com/projects/lmd/)
- Colin Raffel. *Learning-Based Methods for Comparing Sequences, with Applications to Audio-to-MIDI Alignment and Matching.* PhD thesis, 2016. [PDF](http://colinraffel.com/publications/thesis.pdf)

The dataset author notes that MIDI copyright meta-events are inconsistent, so per-file original transcribers are not always attributable.

---

## Optional synthetic MIDI (`demo_abba.mid`)

For a **tiny** multi-track test file (no LMD), generate:

```bash
python scripts/write_example_midi.py
```

That writes `demo_abba.mid` — an **original** pop-style sketch; it is **not** the same as the LMD example above and is **not** used as the default Space example.
