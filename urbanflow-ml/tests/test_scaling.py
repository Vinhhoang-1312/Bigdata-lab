from __future__ import annotations

import unittest

import numpy as np

from forecasting.scaling import RobustZoneScaler


class RobustZoneScalerTest(unittest.TestCase):
    def test_round_trip_restores_non_negative_counts(self):
        counts = np.array(
            [
                [0.0, 5.0],
                [1.0, 5.0],
                [3.0, 5.0],
                [7.0, 5.0],
            ],
            dtype=np.float32,
        )

        scaler = RobustZoneScaler.fit(counts)
        restored = scaler.inverse_transform(scaler.transform(counts))

        np.testing.assert_allclose(restored, counts, rtol=1e-5, atol=1e-5)
        self.assertEqual(float(scaler.scale[1]), 1.0)

    def test_fit_uses_only_the_array_it_receives(self):
        train = np.array([[0.0], [1.0], [2.0]], dtype=np.float32)
        future_outlier = np.array([[100000.0]], dtype=np.float32)

        train_scaler = RobustZoneScaler.fit(train)
        leaked_scaler = RobustZoneScaler.fit(np.concatenate([train, future_outlier], axis=0))

        self.assertNotEqual(float(train_scaler.center[0]), float(leaked_scaler.center[0]))

    def test_inverse_quantiles_broadcasts_over_time_and_quantile_axes(self):
        scaler = RobustZoneScaler(
            center=np.array([0.0, np.log(2.0)], dtype=np.float32),
            scale=np.array([1.0, 1.0], dtype=np.float32),
        )
        scaled = np.zeros((2, 2, 3), dtype=np.float32)

        restored = scaler.inverse_quantiles(scaled)

        np.testing.assert_allclose(restored[:, 0], 0.0)
        np.testing.assert_allclose(restored[:, 1], 1.0)

    def test_inverse_quantiles_scales_zone_axis_and_preserves_quantile_axis(self):
        scaler = RobustZoneScaler(
            center=np.array([0.0, 1.0], dtype=np.float32),
            scale=np.array([1.0, 2.0], dtype=np.float32),
        )
        restored = scaler.inverse_quantiles(np.zeros((1, 2, 3), dtype=np.float32))
        self.assertEqual((1, 2, 3), restored.shape)
        np.testing.assert_allclose(restored[0, 0], [0.0, 0.0, 0.0])
        np.testing.assert_allclose(restored[0, 1], [np.expm1(1.0)] * 3)


if __name__ == "__main__":
    unittest.main()
