{-# LANGUAGE FlexibleContexts, UndecidableInstances #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE EmptyDataDecls #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleInstances #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}
module Database.Persist.Zookeeper.PathPiece
    where

import qualified Database.Zookeeper as Z
import Database.Persist
import Database.Persist.Zookeeper.Store
import Database.Persist.Zookeeper.Internal
import qualified Data.Text as T
import Web.PathPieces (PathPiece (..))
import Control.Applicative
import Data.Monoid


