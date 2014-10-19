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
import Database.Persist.Zookeeper.ZooUtil
import Web.PathPieces (PathPiece (..))
import Control.Applicative

-- | ToPathPiece is used to convert a key to/from text
instance PathPiece (BackendKey Z.Zookeeper) where
  fromPathPiece txt = pure $ ZooKey txt
  toPathPiece (ZooKey txt) = txt


